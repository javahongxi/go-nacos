package cluster

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	"github.com/javahongxi/go-nacos/internal/raftnode"
	"github.com/javahongxi/go-nacos/internal/wire"
	"github.com/javahongxi/sweet/gen"
)

// Applier is the leader-side write path (config.Service): apply forwarded
// command bytes and answer with the domain result, opaque to this package.
type Applier interface {
	ApplyForwarded(data []byte) ([]byte, error)
}

// Server hosts the internal Cluster service on its own listener.
type Server struct {
	node    *raftnode.Node
	applier Applier
	grpc    *grpc.Server
	lis     net.Listener
	logger  *slog.Logger
}

func NewServer(node *raftnode.Node, applier Applier, lis net.Listener, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{node: node, applier: applier, lis: lis, logger: logger}
	s.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(maxMsgSize))
	s.grpc.RegisterService(&clusterDesc, s)
	return s
}

func (s *Server) Serve() error { return s.grpc.Serve(s.lis) }

// Addr is the bound internal address — the node's cluster identity.
func (s *Server) Addr() string { return s.lis.Addr().String() }

func (s *Server) Stop() { s.grpc.GracefulStop() }

// handleJoin adds the caller as a voter. Only the leader mutates membership;
// a follower answers with the leader's id (its sync address, since ServerID
// IS the sync address) so the joiner can retry there.
func (s *Server) handleJoin(_ context.Context, p *gen.Payload) (*gen.Payload, error) {
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
	// A restart with existing state re-joins idempotently.
	for _, srv := range s.node.Voters() {
		if string(srv.ID) == req.NodeID {
			if string(srv.Address) == req.RaftAddr {
				return wire.NewPayload(TypeJoinResponse, JoinResponse{OK: true})
			}
			return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: "node id already registered with a different raft address"})
		}
	}
	f := r.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return wire.NewPayload(TypeJoinResponse, JoinResponse{Message: err.Error()})
	}
	s.logger.Info("cluster: voter added", "node", req.NodeID, "raft", req.RaftAddr)
	return wire.NewPayload(TypeJoinResponse, JoinResponse{OK: true})
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

// clusterAPI is the HandlerType the ServiceDesc checks against — the same
// hand-written-desc pattern as grpcserver (no protoc-gen-go-grpc).
type clusterAPI interface {
	handleJoin(context.Context, *gen.Payload) (*gen.Payload, error)
	handleForward(context.Context, *gen.Payload) (*gen.Payload, error)
}

var clusterDesc = grpc.ServiceDesc{
	ServiceName: "Cluster",
	HandlerType: (*clusterAPI)(nil),
	Metadata:    "gonacos_cluster.proto",
	Methods: []grpc.MethodDesc{
		{
			MethodName: "join",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(gen.Payload)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(clusterAPI).handleJoin(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/Cluster/join"}
				return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
					return srv.(clusterAPI).handleJoin(ctx, req.(*gen.Payload))
				})
			},
		},
		{
			MethodName: "forward",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(gen.Payload)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(clusterAPI).handleForward(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/Cluster/forward"}
				return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
					return srv.(clusterAPI).handleForward(ctx, req.(*gen.Payload))
				})
			},
		},
	},
}