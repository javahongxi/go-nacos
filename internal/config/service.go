// Package config is the config domain: every write goes through raft.Apply,
// reads come from the FSM's memory, and change notifications are key-only
// pushes to the connections that listen on the key.
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/wire"
)

// Key identifies one config: dataId + group + tenant(namespace).
type Key struct {
	DataID string
	Group  string
	Tenant string
}

func (k Key) String() string { return k.DataID + "\x00" + k.Group + "\x00" + k.Tenant }

const applyTimeout = 5 * time.Second

// Forwarder carries a write to the leader's internal address (implemented by
// cluster.Client). nil means single-node: a non-leader write simply fails.
type Forwarder interface {
	Forward(ctx context.Context, leaderAddr string, data []byte) ([]byte, error)
}

// Service owns the listen table: groupKey → connID → last md5 the client
// holds. Writes are replicated; listens are connection-local (a reconnect
// re-listens, so they need no replication). Change notifications fire from
// the FSM hook on every node, each pushing to its own listeners.
type Service struct {
	r      *raft.Raft
	fsm    *FSM
	conns  *conn.Registry
	fwd    Forwarder
	logger *slog.Logger

	mu      sync.Mutex
	listens map[string]map[string]string
}

func NewService(r *raft.Raft, fsm *FSM, conns *conn.Registry, logger *slog.Logger, fwd Forwarder) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		r:       r,
		fsm:     fsm,
		conns:   conns,
		fwd:     fwd,
		logger:  logger,
		listens: map[string]map[string]string{},
	}
	fsm.NotifyOnChange(func(ks string) { s.notify(parseKey(ks)) })
	conns.OnClose(s.removeConn)
	return s
}

func (s *Service) Query(k Key) (Item, bool) { return s.fsm.Get(k.String()) }

// Publish replicates a write; the FSM change hook notifies listeners on
// every node when the content changed.
func (s *Service) Publish(k Key, content, casMD5, typ, appName string) ApplyResult {
	return s.apply(Command{
		Op:      "publish",
		Key:     k.String(),
		Content: content,
		CasMD5:  casMD5,
		Type:    typ,
		AppName: appName,
		Now:     time.Now().UnixMilli(),
	})
}

// Remove replicates a delete. Idempotent; always notifies (Nacos fires the
// event on remove regardless of existence).
func (s *Service) Remove(k Key) ApplyResult {
	return s.apply(Command{Op: "remove", Key: k.String(), Now: time.Now().UnixMilli()})
}

// apply runs a write locally on the leader, or forwards the original command
// bytes to the leader from a follower (the SDK has no redirect semantics).
func (s *Service) apply(cmd Command) ApplyResult {
	data, err := json.Marshal(cmd)
	if err != nil {
		return ApplyResult{Message: err.Error()}
	}
	if s.r.State() != raft.Leader {
		return s.forward(data)
	}
	res, err := s.applyRaft(data)
	if errors.Is(err, raft.ErrNotLeader) {
		return s.forward(data) // lost leadership mid-apply
	}
	if err != nil {
		return ApplyResult{Message: fmt.Sprintf("config: raft apply: %v", err)}
	}
	return res
}

func (s *Service) applyRaft(data []byte) (ApplyResult, error) {
	f := s.r.Apply(data, applyTimeout)
	if err := f.Error(); err != nil {
		return ApplyResult{}, err
	}
	res, ok := f.Response().(ApplyResult)
	if !ok {
		return ApplyResult{}, errors.New("config: unexpected apply response")
	}
	return res, nil
}

func (s *Service) forward(data []byte) ApplyResult {
	if s.fwd == nil {
		return ApplyResult{Message: "config: not the leader"}
	}
	_, leaderID := s.r.LeaderWithID()
	if leaderID == "" {
		return ApplyResult{Message: "config: no leader"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), applyTimeout)
	defer cancel()
	out, err := s.fwd.Forward(ctx, string(leaderID), data)
	if err != nil {
		return ApplyResult{Message: fmt.Sprintf("config: forward to leader: %v", err)}
	}
	var res ApplyResult
	if err := json.Unmarshal(out, &res); err != nil {
		return ApplyResult{Message: fmt.Sprintf("config: decode forward result: %v", err)}
	}
	return res
}

// ApplyForwarded is the leader side of a forwarded write: apply the original
// command bytes as-is (the origin already stamped Now) and answer with the
// result. Every node's FSM hook notifies its own listeners, so the leader
// needs no extra fan-out.
func (s *Service) ApplyForwarded(data []byte) ([]byte, error) {
	if s.r.State() != raft.Leader {
		return nil, errors.New("config: not the leader")
	}
	res, err := s.applyRaft(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

// Listen registers (or drops) a batch of watched keys for one connection and
// returns the stale subset immediately — md5 equality is string equality, and
// a missing config's md5 is "" (ConfigCacheService.isUptodate).
func (s *Service) Listen(connID string, contexts []wire.ConfigListenContext, listen bool) []wire.ConfigContext {
	var changed []wire.ConfigContext
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lc := range contexts {
		k := Key{DataID: lc.DataID, Group: lc.Group, Tenant: lc.Tenant}
		ks := k.String()
		if !listen {
			if m := s.listens[ks]; m != nil {
				delete(m, connID)
			}
			continue
		}
		m := s.listens[ks]
		if m == nil {
			m = map[string]string{}
			s.listens[ks] = m
		}
		m[connID] = lc.MD5
		serverMD5 := ""
		if it, ok := s.fsm.Get(ks); ok {
			serverMD5 = it.MD5
		}
		if serverMD5 != lc.MD5 {
			changed = append(changed, wire.ConfigContext{Group: k.Group, DataID: k.DataID, Tenant: k.Tenant})
		}
	}
	return changed
}

// notify pushes the key to every listener. No md5 filtering here — Nacos
// pushes to all listeners of the key and lets the client's md5 guard decide
// whether to re-query (RpcConfigChangeNotifier).
func (s *Service) notify(k Key) {
	ks := k.String()
	s.mu.Lock()
	var ids []string
	for id := range s.listens[ks] {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		c := s.conns.GetByID(id)
		if c == nil {
			continue
		}
		go func(c *conn.Connection) {
			reqID := c.NextRequestID()
			p, err := wire.NewPayload(wire.TypeConfigChangeNotifyRequest, wire.ConfigChangeNotifyRequest{
				RequestID: reqID,
				DataID:    k.DataID,
				Group:     k.Group,
				Tenant:    k.Tenant,
			})
			if err != nil {
				return
			}
			if err := s.conns.Push(c, p, reqID); err != nil {
				s.logger.Warn("config notify failed", "conn", c.ID, "dataId", k.DataID, "err", err)
			}
		}(c)
	}
}

// removeConn drops every listen the connection held.
func (s *Service) removeConn(connID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ks, m := range s.listens {
		delete(m, connID)
		if len(m) == 0 {
			delete(s.listens, ks)
		}
	}
}

// Info is a read-only config summary for the admin console (content omitted).
type Info struct {
	DataID       string `json:"dataId"`
	Group        string `json:"group"`
	Tenant       string `json:"tenant"`
	MD5          string `json:"md5"`
	Type         string `json:"type,omitempty"`
	AppName      string `json:"appName,omitempty"`
	LastModified int64  `json:"lastModified"`
}

// List returns every config as a summary, ordered by key. Read-only.
func (s *Service) List() []Info {
	all := s.fsm.All()
	out := make([]Info, 0, len(all))
	for ks, it := range all {
		k := parseKey(ks)
		out = append(out, Info{
			DataID: k.DataID, Group: k.Group, Tenant: k.Tenant,
			MD5: it.MD5, Type: it.Type, AppName: it.AppName, LastModified: it.LastModified,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DataID != out[j].DataID {
			return out[i].DataID < out[j].DataID
		}
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Tenant < out[j].Tenant
	})
	return out
}

// parseKey reverses Key.String's \x00 joining.
func parseKey(s string) Key {
	p := strings.SplitN(s, "\x00", 3)
	var k Key
	if len(p) > 0 {
		k.DataID = p[0]
	}
	if len(p) > 1 {
		k.Group = p[1]
	}
	if len(p) > 2 {
		k.Tenant = p[2]
	}
	return k
}
