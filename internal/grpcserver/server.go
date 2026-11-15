// Package grpcserver implements Nacos' two gRPC services with hand-written
// ServiceDescs — no protoc-gen-go-grpc, the same toolchain choice sweet made
// on the client side. The envelope types come from sweet/gen: sharing the one
// generated package keeps a single protobuf global registration (two copies
// of the same .proto panic at init) and guarantees the wire-twin.
package grpcserver

import (
	"context"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/javahongxi/go-nacos/internal/config"
	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/sweet/gen"
)

type Server struct {
	conns  *conn.Registry
	cfg    *config.Service
	naming *naming.Service
	grpc   *grpc.Server
	logger *slog.Logger
}

func New(conns *conn.Registry, cfg *config.Service, naming *naming.Service, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{conns: conns, cfg: cfg, naming: naming, logger: logger}
	s.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(10 << 20))
	s.grpc.RegisterService(&requestDesc, s)
	s.grpc.RegisterService(&biStreamDesc, s)
	return s
}

// serverAPI is the HandlerType both ServiceDescs check against — grpc requires
// an interface here (it does reflect.TypeOf(HandlerType).Elem().Implements).
// *Server satisfies it; the method set is exactly what the generated stubs
// would have declared.
type serverAPI interface {
	handleUnary(context.Context, *gen.Payload) (*gen.Payload, error)
	handleBiStream(grpc.ServerStream) error
}

func (s *Server) Serve(lis net.Listener) error { return s.grpc.Serve(lis) }

// Stop retires connections first: GracefulStop waits for in-flight RPCs, and
// a bidi stream never ends on its own — without CloseAll it would hang.
func (s *Server) Stop() {
	s.conns.CloseAll()
	s.grpc.GracefulStop()
}

// HardStop aborts without retiring connections or waiting for streams — the
// e2e crash simulation: peers must notice the death through their own ping
// timeout, not through goodbye broadcasts.
func (s *Server) HardStop() { s.grpc.Stop() }

// DropConnections retires every live connection without stopping the server;
// the e2e reconnect tests use it to force sweet's replay path.
func (s *Server) DropConnections() { s.conns.CloseAll() }

// peerFrom extracts the remote address — the connection identity shared by
// unary calls and the bidi stream on the same TCP connection.
func peerFrom(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return ""
}

var requestDesc = grpc.ServiceDesc{
	ServiceName: "Request",
	HandlerType: (*serverAPI)(nil),
	Metadata:    "nacos_grpc_service.proto",
	Methods: []grpc.MethodDesc{{
		MethodName: "request",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(gen.Payload)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(serverAPI).handleUnary(ctx, in)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/Request/request"}
			return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
				return srv.(serverAPI).handleUnary(ctx, req.(*gen.Payload))
			})
		},
	}},
}

var biStreamDesc = grpc.ServiceDesc{
	ServiceName: "BiRequestStream",
	HandlerType: (*serverAPI)(nil),
	Metadata:    "nacos_grpc_service.proto",
	Streams: []grpc.StreamDesc{{
		StreamName:    "requestBiStream",
		ClientStreams: true,
		ServerStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(serverAPI).handleBiStream(stream)
		},
	}},
}
