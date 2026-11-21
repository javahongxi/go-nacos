// Package cluster is the internal node-to-node transport: a small gRPC
// service registered on the SDK gRPC server (same port, separate service —
// Java Nacos multiplexes its server gRPC the same way). hashicorp/raft's
// transport is consensus-only (r-nacos rides its own raft network layer; we
// cannot), so join/forward — and M2's naming sync — live here. Envelopes
// reuse sweet/gen.Payload with JSON bodies, the same wire style as the SDK
// path and r-nacos' JSON sync payloads.
package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/go-nacos/internal/wire"
	"github.com/javahongxi/sweet/gen"
)

// Snapshots can be large; match the server's receive cap.
const maxMsgSize = 32 << 20

// Metadata.type values for the internal protocol (not Nacos SDK types).
const (
	TypeJoinRequest      = "JoinRequest"
	TypeJoinResponse     = "JoinResponse"
	TypeRemoveRequest    = "RemoveRequest"
	TypeRemoveResponse   = "RemoveResponse"
	TypeForwardRequest   = "ForwardRequest"
	TypeForwardResponse  = "ForwardResponse"
	TypeSyncResponse     = "SyncResponse"
	TypeSnapshotRequest  = "SnapshotRequest"
	TypeSnapshotResponse = "SnapshotResponse"
	TypeRaftIndexRequest  = "RaftIndexRequest"
	TypeRaftIndexResponse = "RaftIndexResponse"
)

// JoinRequest asks the cluster to add the caller as a voter — via a learner
// (nonvoter) catch-up phase, so the quorum only grows once the log is replicated.
// NodeID is the caller's gRPC address (its cluster identity), RaftAddr its raft
// transport.
type JoinRequest struct {
	NodeID   string `json:"nodeId"`
	RaftAddr string `json:"raftAddr"`
}

// JoinResponse: OK on acceptance; otherwise LeaderID redirects the caller to
// the current leader (empty when no leader is known yet — retry the same peer).
type JoinResponse struct {
	OK       bool   `json:"ok"`
	LeaderID string `json:"leaderId,omitempty"`
	Message  string `json:"message,omitempty"`
}

// RemoveRequest asks the leader to drop a voter (scale-down). NodeID is the
// target's cluster identity (its gRPC address).
type RemoveRequest struct {
	NodeID string `json:"nodeId"`
}

// RemoveResponse: OK on removal (or when the target is already absent);
// otherwise LeaderID redirects the caller to the current leader.
type RemoveResponse struct {
	OK       bool   `json:"ok"`
	LeaderID string `json:"leaderId,omitempty"`
	Message  string `json:"message,omitempty"`
}

// ForwardRequest carries one replicated write (opaque domain command bytes).
type ForwardRequest struct {
	Data []byte `json:"data"`
}

// ForwardResponse carries the leader's apply result bytes, or a Message on
// refusal (e.g. the peer lost leadership mid-flight).
type ForwardResponse struct {
	Data    []byte `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
}

// SyncResponse acknowledges one naming sync message.
type SyncResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// SnapshotRequest asks a peer for its full naming state.
type SnapshotRequest struct{}

// SnapshotResponse carries the state as domain-owned JSON bytes.
type SnapshotResponse struct {
	Data    []byte `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
}

// RaftIndexRequest asks a node for its last stored raft log index.
type RaftIndexRequest struct{}

// RaftIndexResponse reports the node's raft LastIndex — the leader's
// catch-up gate polls it while a joiner is still a nonvoter.
type RaftIndexResponse struct {
	LastIndex uint64 `json:"lastIndex"`
}

// Client dials peers lazily and caches one connection per address.
type Client struct {
	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	logger *slog.Logger
}

func NewClient(logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{conns: map[string]*grpc.ClientConn{}, logger: logger}
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc := c.conns[addr]; cc != nil {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

func (c *Client) invoke(ctx context.Context, addr, method, typeName string, body any, out any) error {
	cc, err := c.conn(addr)
	if err != nil {
		return err
	}
	p, err := wire.NewPayload(typeName, body)
	if err != nil {
		return err
	}
	resp := new(gen.Payload)
	if err := cc.Invoke(ctx, method, p, resp); err != nil {
		return err
	}
	return wire.Decode(resp, out)
}

// Join asks a peer to add us as a voter, following leader redirects.
func (c *Client) Join(ctx context.Context, addr string, req JoinRequest) (JoinResponse, error) {
	var resp JoinResponse
	err := c.invoke(ctx, addr, "/Cluster/join", TypeJoinRequest, req, &resp)
	return resp, err
}

// Remove asks a peer to drop a voter; the caller follows leader redirects.
func (c *Client) Remove(ctx context.Context, addr string, req RemoveRequest) (RemoveResponse, error) {
	var resp RemoveResponse
	err := c.invoke(ctx, addr, "/Cluster/remove", TypeRemoveRequest, req, &resp)
	return resp, err
}

// Forward sends a write to the leader; the answer is the domain's apply
// result, opaque to this package.
func (c *Client) Forward(ctx context.Context, addr string, data []byte) ([]byte, error) {
	var resp ForwardResponse
	if err := c.invoke(ctx, addr, "/Cluster/forward", TypeForwardRequest, ForwardRequest{Data: data}, &resp); err != nil {
		return nil, err
	}
	if resp.Message != "" {
		return nil, errors.New(resp.Message)
	}
	return resp.Data, nil
}

// Send delivers one naming sync message: typeName (SyncInstances/RemoveConn/
// FullMapping/Ping) travels in Metadata.type, the JSON body is domain-owned.
func (c *Client) Send(ctx context.Context, addr, typeName string, body any) error {
	var resp SyncResponse
	if err := c.invoke(ctx, addr, "/Cluster/sync", typeName, body, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Message)
	}
	return nil
}

// PullSnapshot fetches a peer's full naming state (join/restart catch-up).
func (c *Client) PullSnapshot(ctx context.Context, addr string) ([]byte, error) {
	var resp SnapshotResponse
	if err := c.invoke(ctx, addr, "/Cluster/snapshot", TypeSnapshotRequest, SnapshotRequest{}, &resp); err != nil {
		return nil, err
	}
	if resp.Message != "" {
		return nil, errors.New(resp.Message)
	}
	return resp.Data, nil
}

// RaftIndex fetches a peer's last stored raft log index (join catch-up gate).
func (c *Client) RaftIndex(ctx context.Context, addr string) (uint64, error) {
	var resp RaftIndexResponse
	if err := c.invoke(ctx, addr, "/Cluster/raftIndex", TypeRaftIndexRequest, RaftIndexRequest{}, &resp); err != nil {
		return 0, err
	}
	return resp.LastIndex, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		_ = cc.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}