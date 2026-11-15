package grpcserver

import (
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"

	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/wire"
	"github.com/javahongxi/sweet/gen"
)

// handleBiStream owns one bidi stream: the ConnectionSetupRequest handshake,
// then routing client Response frames to pending pushes. Stream end retires
// the connection — unless a newer stream already replaced it, in which case
// Unregister's id guard makes the cleanup a no-op.
func (s *Server) handleBiStream(stream grpc.ServerStream) error {
	ctx := stream.Context()
	peerAddr := peerFrom(ctx)

	// One sender at a time: the handshake ack and domain pushes share the
	// stream, and gRPC forbids concurrent SendMsg.
	var sendMu sync.Mutex
	send := func(p *gen.Payload) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.SendMsg(p)
	}

	type recvResult struct {
		p   *gen.Payload
		err error
	}
	recvCh := make(chan recvResult)
	go func() {
		for {
			p := new(gen.Payload)
			err := stream.RecvMsg(p)
			// The select keeps this goroutine from leaking on a channel
			// nobody reads after the handler returns.
			select {
			case recvCh <- recvResult{p: p, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	var current *conn.Connection
	var done <-chan struct{} // nil until setup; a nil channel never fires
	defer func() {
		if current != nil {
			s.conns.Unregister(peerAddr, current.ID)
		}
	}()

	for {
		var res recvResult
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil // retired: replaced by a newer stream, or server shutdown
		case res = <-recvCh:
		}
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				return nil
			}
			return res.err
		}
		p := res.p
		switch wire.Type(p) {
		case wire.TypeConnectionSetupRequest:
			var req wire.ConnectionSetupRequest
			if err := wire.Decode(p, &req); err != nil {
				s.logger.Warn("undecodable setup request", "peer", peerAddr, "err", err)
				continue
			}
			meta := conn.Meta{
				ClientIP: p.GetMetadata().GetClientIp(),
				Version:  req.ClientVersion,
				Module:   req.Labels["module"],
				AppName:  req.Labels["appName"],
				Labels:   req.Labels,
			}
			current = s.conns.Register(peerAddr, meta, send)
			done = current.Done()
			s.logger.Debug("connection setup", "conn", current.ID, "module", meta.Module, "version", meta.Version)
			// Sent without waiting for SetupAckResponse, exactly like
			// GrpcBiStreamRequestAcceptor's sendRequestNoAck.
			ack, err := wire.NewPayload(wire.TypeSetupAckRequest, wire.SetupAckRequest{AbilityTable: map[string]bool{}})
			if err != nil {
				return err
			}
			if err := send(ack); err != nil {
				return err
			}
		default:
			// Response frames (SetupAckResponse, *NotifyResponse, ...): the
			// only field that matters is the requestId being acked.
			if current == nil {
				continue
			}
			var body struct {
				RequestID string `json:"requestId"`
			}
			if err := wire.Decode(p, &body); err == nil && body.RequestID != "" {
				current.Ack(body.RequestID)
			}
		}
	}
}
