// Package fsm is the composite raft state machine: one raft group carries
// every CP domain (config writes, naming persistent instances), each log
// entry an Envelope naming its domain. Durability is raft's own log+snapshot
// (r-nacos / jaws-harbor shape) — no side database. The Writer is the shared
// apply-or-forward path both domains use, riding the existing /Cluster/forward
// RPC for follower writes.
package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// Domain names, the Envelope discriminators.
const (
	DomainConfig = "config"
	DomainNaming = "naming"
)

// ApplyTimeout bounds one replicated write (local apply or leader forward).
const ApplyTimeout = 5 * time.Second

// barrierPayload is the forward sentinel for Barrier: the leader runs a raft
// barrier instead of applying it (a barrier never enters the log as data).
var barrierPayload = []byte(`{"barrier":true}`)

// Domain is one replicated state owner. ApplyData consumes the Envelope's
// raw command bytes and answers with the domain's own result type; the
// snapshot pair moves JSON-marshalable state through the Router.
type Domain interface {
	ApplyData(data []byte) any
	SnapshotData() any
	RestoreData(data []byte) error
}

// Envelope wraps every raft log entry: the domain name plus its opaque
// command bytes, so one raft group can carry heterogeneous writes.
type Envelope struct {
	Domain string          `json:"domain"`
	Data   json.RawMessage `json:"data"`
}

// Router implements raft.FSM by dispatching to the registered domains.
type Router struct {
	domains map[string]Domain
}

func NewRouter(domains map[string]Domain) *Router {
	return &Router{domains: domains}
}

func (r *Router) Apply(l *raft.Log) any {
	var env Envelope
	if err := json.Unmarshal(l.Data, &env); err != nil {
		return fmt.Errorf("fsm: undecodable envelope: %w", err)
	}
	d := r.domains[env.Domain]
	if d == nil {
		return fmt.Errorf("fsm: unknown domain %q", env.Domain)
	}
	return d.ApplyData(env.Data)
}

func (r *Router) Snapshot() (raft.FSMSnapshot, error) {
	state := make(map[string]any, len(r.domains))
	for name, d := range r.domains {
		state[name] = d.SnapshotData()
	}
	return &routerSnapshot{state: state}, nil
}

func (r *Router) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state map[string]json.RawMessage
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	for name, data := range state {
		d := r.domains[name]
		if d == nil {
			return fmt.Errorf("fsm: snapshot carries unknown domain %q", name)
		}
		if err := d.RestoreData(data); err != nil {
			return err
		}
	}
	return nil
}

type routerSnapshot struct {
	state map[string]any
}

func (s *routerSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *routerSnapshot) Release() {}

// Forwarder carries a write to the leader's internal address (implemented by
// cluster.Client). nil means single-node: a non-leader write simply fails.
type Forwarder interface {
	Forward(ctx context.Context, leaderAddr string, data []byte) ([]byte, error)
}

// Writer is the shared write path: apply locally on the leader, forward the
// envelope bytes from a follower (the SDK has no redirect semantics). It also
// serves the leader side of a forward as cluster.Applier.
type Writer struct {
	r   *raft.Raft
	fwd Forwarder
}

func NewWriter(r *raft.Raft, fwd Forwarder) *Writer {
	return &Writer{r: r, fwd: fwd}
}

// Apply replicates one domain command and decodes the domain's result. The
// leader applies directly; a follower (or a leader that lost leadership
// mid-apply) forwards the original envelope bytes to the leader.
func Apply[T any](w *Writer, domain string, cmd any) (T, error) {
	var zero T
	data, err := json.Marshal(cmd)
	if err != nil {
		return zero, err
	}
	env, err := json.Marshal(Envelope{Domain: domain, Data: data})
	if err != nil {
		return zero, err
	}
	if w.r.State() == raft.Leader {
		resp, err := w.applyRaft(env)
		if errors.Is(err, raft.ErrNotLeader) {
			return forward[T](w, env) // lost leadership mid-apply
		}
		if err != nil {
			return zero, err
		}
		res, ok := resp.(T)
		if !ok {
			if e, isErr := resp.(error); isErr {
				return zero, e
			}
			return zero, fmt.Errorf("fsm: unexpected apply response %T", resp)
		}
		return res, nil
	}
	return forward[T](w, env)
}

func (w *Writer) applyRaft(data []byte) (any, error) {
	f := w.r.Apply(data, ApplyTimeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Response(), nil
}

func forward[T any](w *Writer, env []byte) (T, error) {
	var res T
	if w.fwd == nil {
		return res, errors.New("fsm: not the leader")
	}
	_, leaderID := w.r.LeaderWithID()
	if leaderID == "" {
		return res, errors.New("fsm: no leader")
	}
	ctx, cancel := context.WithTimeout(context.Background(), ApplyTimeout)
	defer cancel()
	out, err := w.fwd.Forward(ctx, string(leaderID), env)
	if err != nil {
		return res, fmt.Errorf("fsm: forward to leader: %w", err)
	}
	if err := json.Unmarshal(out, &res); err != nil {
		return res, fmt.Errorf("fsm: decode forward result: %w", err)
	}
	return res, nil
}

// Barrier blocks until every log entry committed before this call has been
// applied to the local FSM. NewRaft only restores snapshots; the tail log
// replays asynchronously once a leader commits it, so a restarted node must
// barrier before serving reads. A follower forwards the sentinel so the
// leader's barrier commits the whole tail log; the follower's own replay
// rides the same AppendEntries stream right behind it.
func (w *Writer) Barrier(ctx context.Context) error {
	timeout := ApplyTimeout
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && d < timeout {
			timeout = d
		}
	}
	if w.r.State() == raft.Leader {
		return w.r.Barrier(timeout).Error()
	}
	if w.fwd == nil {
		return errors.New("fsm: not the leader")
	}
	_, leaderID := w.r.LeaderWithID()
	if leaderID == "" {
		return errors.New("fsm: no leader")
	}
	_, err := w.fwd.Forward(ctx, string(leaderID), barrierPayload)
	return err
}

// ApplyForwarded is the leader side of a forwarded write (cluster.Applier):
// apply the original envelope bytes as-is (the origin already stamped Now)
// and answer with the domain result. Every node's FSM applies the same entry,
// so change hooks fire cluster-wide with no extra fan-out.
func (w *Writer) ApplyForwarded(data []byte) ([]byte, error) {
	if w.r.State() != raft.Leader {
		return nil, errors.New("fsm: not the leader")
	}
	if bytes.Equal(data, barrierPayload) {
		if err := w.r.Barrier(ApplyTimeout).Error(); err != nil {
			return nil, err
		}
		return []byte(`{}`), nil
	}
	resp, err := w.applyRaft(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}