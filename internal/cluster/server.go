package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	"github.com/javahongxi/go-nacos/internal/raftnode"
	"github.com/javahongxi/go-nacos/internal/wire"
	"github.com/javahongxi/sweet/gen"
)

// Applier is the leader-side write path (fsm.Writer): apply forwarded
// envelope bytes and answer with the domain result, opaque to this package.
type Applier interface {
	ApplyForwarded(data []byte) ([]byte, error)
}

// Syncer is the naming sync manager: apply one inbound sync message and
// serve full-state snapshot pulls. nil disables naming sync (single-node).
type Syncer interface {
	HandleSync(typeName string, data []byte) error
	Snapshot() ([]byte, error)
}

// Server hosts the internal Cluster service. It owns no listener: the app
// registers it on the SDK gRPC server, so node-to-node traffic rides the same
// port as client traffic (Java Nacos multiplexes its server gRPC the same
// way). Lifecycle belongs to that server.
type Server struct {
	node    *raftnode.Node
	client  *Client // probes the joiner's log progress during the learner phase
	applier Applier
	syncer  Syncer
	logger  *slog.Logger

	catchUpPoll    time.Duration // learner catch-up probe cadence
	catchUpTimeout time.Duration // promote anyway once this budget runs out
}

// Option tunes a Server at construction.
type Option func(*Server)

// WithCatchUp overrides the learner catch-up gate timing (zero values keep
// the defaults: 100ms poll, 10s budget). Tests widen the poll to make the
// learner window observable.
func WithCatchUp(poll, timeout time.Duration) Option {
	return func(s *Server) {
		if poll > 0 {
			s.catchUpPoll = poll
		}
		if timeout > 0 {
			s.catchUpTimeout = timeout
		}
	}
}

func NewServer(node *raftnode.Node, client *Client, applier Applier, syncer Syncer, logger *slog.Logger, opts ...Option) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		node: node, client: client, applier: applier, syncer: syncer, logger: logger,
		catchUpPoll: 100 * time.Millisecond, catchUpTimeout: 10 * time.Second,
	}
	for _, f := range opts {
		f(s)
	}
	return s
}

// Register attaches the Cluster service to an existing gRPC server.
func (s *Server) Register(g *grpc.Server) { g.RegisterService(&clusterDesc, s) }

// handleJoin admits the caller in two steps: AddNonvoter (learner — receives
// the log but does not vote, so the quorum is unchanged), then promotion to
// voter once the joiner's log has caught up. Only the leader mutates
// membership; a follower answers with the leader's id (its gRPC address,
// since ServerID IS the gRPC address) so the joiner can retry there.
func (s *Server) handleJoin(ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
	var req JoinRequest
	if err := wire.Decode(p, &req); err != nil {
		return nil, err
	}
	if req.NodeID == "" || req.RaftAddr == "" {
		return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: "nodeId and raftAddr are required"})
	}
	r := s.node.Raft
	if r.State() != raft.Leader {
		_, leaderID := r.LeaderWithID()
		return wire.NewPayload(TypeJoinResponse, JoinResponse{LeaderID: string(leaderID), Message: "not leader"})
	}
	// A restart with existing state re-joins idempotently; a stranded
	// nonvoter (an interrupted earlier join) resumes at the catch-up gate.
	learner := false
	for _, srv := range s.node.Voters() {
		if string(srv.ID) == req.NodeID {
			if string(srv.Address) != req.RaftAddr {
				return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: "node id already registered with a different raft address"})
			}
			if srv.Suffrage == raft.Voter {
				return wire.NewPayload(TypeJoinResponse, JoinResponse{OK: true})
			}
			learner = true
			break
		}
	}
	if !learner {
		f := r.AddNonvoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 10*time.Second)
		if err := f.Error(); err != nil {
			return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: err.Error()})
		}
		s.logger.Info("cluster: learner added", "node", req.NodeID, "raft", req.RaftAddr)
	}
	// Catch-up gate: the joiner must hold the log as of admission before it
	// votes. hashicorp/raft exposes no per-peer matchIndex, so poll the
	// joiner's own LastIndex over the internal gRPC channel.
	target := r.LastIndex()
	caughtUp, leaderID := s.waitCatchUp(ctx, req.NodeID, target)
	if leaderID != "" { // lost leadership mid-gate: the joiner retries there
		return wire.NewPayload(TypeJoinResponse, JoinResponse{LeaderID: leaderID, Message: "not leader"})
	}
	if ctx.Err() != nil { // joiner gave up or died: leave it a learner, a later join resumes
		return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: "join canceled during catch-up"})
	}
	if !caughtUp {
		s.logger.Warn("cluster: catch-up timed out, promoting anyway", "node", req.NodeID, "target", target)
	}
	f := r.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: err.Error()})
	}
	s.logger.Info("cluster: voter added", "node", req.NodeID, "raft", req.RaftAddr, "caughtUp", caughtUp)
	return wire.NewPayload(TypeJoinResponse, JoinResponse{OK: true})
}

// waitCatchUp polls the joiner's last log index until it reaches target.
// Returns caughtUp, or a non-empty leaderID when this node lost leadership
// (the caller redirects). Probe errors are retried — the joiner's gRPC may
// still be warming up; the timeout is the backstop.
func (s *Server) waitCatchUp(ctx context.Context, nodeID string, target uint64) (bool, string) {
	deadline := time.Now().Add(s.catchUpTimeout)
	for {
		select {
		case <-ctx.Done():
			return false, ""
		case <-time.After(s.catchUpPoll):
		}
		if s.node.Raft.State() != raft.Leader {
			_, leaderID := s.node.Raft.LeaderWithID()
			return false, string(leaderID)
		}
		probeCtx, cancel := context.WithTimeout(ctx, s.catchUpPoll)
		idx, err := s.client.RaftIndex(probeCtx, nodeID)
		cancel()
		if err == nil && idx >= target {
			return true, ""
		}
		if time.Now().After(deadline) {
			return false, ""
		}
	}
}

// handleRemove drops a voter (scale-down). Like join, only the leader mutates
// membership and followers redirect; removing an absent node is idempotent.
// The last voter is refused: an empty configuration can never elect again.
func (s *Server) handleRemove(_ context.Context, p *gen.Payload) (*gen.Payload, error) {
	var req RemoveRequest
	if err := wire.Decode(p, &req); err != nil {
		return nil, err
	}
	if req.NodeID == "" {
		return wire.NewPayload(TypeRemoveResponse, RemoveResponse{Message: "nodeId is required"})
	}
	r := s.node.Raft
	if r.State() != raft.Leader {
		_, leaderID := r.LeaderWithID()
		return wire.NewPayload(TypeRemoveResponse, RemoveResponse{LeaderID: string(leaderID), Message: "not leader"})
	}
	voters := s.node.Voters()
	found := false
	numVoters := 0
	for _, srv := range voters {
		if srv.Suffrage == raft.Voter {
			numVoters++
		}
		if string(srv.ID) == req.NodeID {
			found = true
		}
	}
	if !found {
		return wire.NewPayload(TypeRemoveResponse, RemoveResponse{OK: true}) // already absent
	}
	// Only voters count towards the quorum — a learner must not mask the
	// last-voter guard.
	if numVoters <= 1 {
		for _, srv := range voters {
			if string(srv.ID) == req.NodeID && srv.Suffrage == raft.Voter {
				return wire.NewPayload(TypeRemoveResponse, RemoveResponse{Message: "cannot remove the last voter"})
			}
		}
	}
	f := r.RemoveServer(raft.ServerID(req.NodeID), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return wire.NewPayload(TypeRemoveResponse, RemoveResponse{Message: err.Error()})
	}
	s.logger.Info("cluster: voter removed", "node", req.NodeID)
	return wire.NewPayload(TypeRemoveResponse, RemoveResponse{OK: true})
}

// handleForward applies a follower's write on the leader. The original
// command bytes are applied as-is (the origin already stamped Now), and the
// FSM change hook notifies listeners on every node — no extra fan-out.
func (s *Server) handleForward(_ context.Context, p *gen.Payload) (*gen.Payload, error) {
	var req ForwardRequest
	if err := wire.Decode(p, &req); err != nil {
		return nil, err
	}
	if s.node.Raft.State() != raft.Leader {
		_, leaderID := s.node.Raft.LeaderWithID()
		return wire.NewPayload(TypeForwardResponse, ForwardResponse{Message: "not leader: " + string(leaderID)})
	}
	data, err := s.applier.ApplyForwarded(req.Data)
	if err != nil {
		return wire.NewPayload(TypeForwardResponse, ForwardResponse{Message: err.Error()})
	}
	return wire.NewPayload(TypeForwardResponse, ForwardResponse{Data: data})
}

// handleSync applies one naming sync message; the payload type names the
// message (SyncInstances/RemoveConn/FullMapping/Ping), the body is JSON.
func (s *Server) handleSync(_ context.Context, p *gen.Payload) (*gen.Payload, error) {
	if s.syncer == nil {
		return wire.NewPayload(TypeSyncResponse, SyncResponse{Message: "naming sync unavailable"})
	}
	if err := s.syncer.HandleSync(wire.Type(p), p.GetBody().GetValue()); err != nil {
		return wire.NewPayload(TypeSyncResponse, SyncResponse{Message: err.Error()})
	}
	return wire.NewPayload(TypeSyncResponse, SyncResponse{OK: true})
}

// handleSnapshot serves a full naming state pull.
func (s *Server) handleSnapshot(_ context.Context, _ *gen.Payload) (*gen.Payload, error) {
	if s.syncer == nil {
		return wire.NewPayload(TypeSnapshotResponse, SnapshotResponse{Message: "naming sync unavailable"})
	}
	data, err := s.syncer.Snapshot()
	if err != nil {
		return wire.NewPayload(TypeSnapshotResponse, SnapshotResponse{Message: err.Error()})
	}
	return wire.NewPayload(TypeSnapshotResponse, SnapshotResponse{Data: data})
}

// handleRaftIndex reports this node's last stored log index — the leader's
// catch-up gate polls the joiner here (matchIndex is not exported by the
// raft library).
func (s *Server) handleRaftIndex(_ context.Context, _ *gen.Payload) (*gen.Payload, error) {
	return wire.NewPayload(TypeRaftIndexResponse, RaftIndexResponse{LastIndex: s.node.Raft.LastIndex()})
}

// clusterAPI is the HandlerType the ServiceDesc checks against — the same
// hand-written-desc pattern as grpcserver (no protoc-gen-go-grpc).
type clusterAPI interface {
	handleJoin(context.Context, *gen.Payload) (*gen.Payload, error)
	handleRemove(context.Context, *gen.Payload) (*gen.Payload, error)
	handleForward(context.Context, *gen.Payload) (*gen.Payload, error)
	handleSync(context.Context, *gen.Payload) (*gen.Payload, error)
	handleSnapshot(context.Context, *gen.Payload) (*gen.Payload, error)
	handleRaftIndex(context.Context, *gen.Payload) (*gen.Payload, error)
}

// unaryMethod builds one MethodDesc; every internal RPC is Payload→Payload.
func unaryMethod(name string, h func(clusterAPI, context.Context, *gen.Payload) (*gen.Payload, error)) grpc.MethodDesc {
	full := "/Cluster/" + name
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(gen.Payload)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return h(srv.(clusterAPI), ctx, in)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: full}
			return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
				return h(srv.(clusterAPI), ctx, req.(*gen.Payload))
			})
		},
	}
}

var clusterDesc = grpc.ServiceDesc{
	ServiceName: "Cluster",
	HandlerType: (*clusterAPI)(nil),
	Metadata:    "gonacos_cluster.proto",
	Methods: []grpc.MethodDesc{
		unaryMethod("join", func(a clusterAPI, ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
			return a.handleJoin(ctx, p)
		}),
		unaryMethod("remove", func(a clusterAPI, ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
			return a.handleRemove(ctx, p)
		}),
		unaryMethod("forward", func(a clusterAPI, ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
			return a.handleForward(ctx, p)
		}),
		unaryMethod("sync", func(a clusterAPI, ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
			return a.handleSync(ctx, p)
		}),
		unaryMethod("snapshot", func(a clusterAPI, ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
			return a.handleSnapshot(ctx, p)
		}),
		unaryMethod("raftIndex", func(a clusterAPI, ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
			return a.handleRaftIndex(ctx, p)
		}),
	},
}