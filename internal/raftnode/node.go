// Package raftnode assembles hashicorp/raft: one bolt file for log+stable
// storage, file snapshots, TCP transport. M1 bootstraps a single-node cluster
// (restart-safe via HasExistingState); M2 adds join and leader routing.
package raftnode

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Node struct {
	Raft      *raft.Raft
	transport *raft.NetworkTransport
	logs      *raftboltdb.BoltStore
	nodeID    string
}

// Option tunes a Node at construction.
type Option func(*options)

type options struct {
	nodeID string
	join   bool
}

// WithNodeID sets the raft ServerID. go-nacos identifies a node by its gRPC
// address (SDK and internal cluster RPCs share it), so the raft configuration
// doubles as the cluster member table: ID = gRPC address (dialable),
// Address = raft transport.
func WithNodeID(id string) Option { return func(o *options) { o.nodeID = id } }

// WithJoin skips bootstrap on a fresh node: an existing leader AddVoters us
// instead (the app drives the join handshake over the internal transport).
func WithJoin() Option { return func(o *options) { o.join = true } }

func New(dataDir, listen string, fsm raft.FSM, opts ...Option) (*Node, error) {
	var o options
	for _, f := range opts {
		f(&o)
	}
	cfg := raft.DefaultConfig()
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "gonacos-raft", Level: hclog.Warn, Output: os.Stderr})

	// nil advertise: the transport reports the actually-bound address, so a
	// :0 listen works in tests and cluster members get dialable addresses.
	transport, err := raft.NewTCPTransport(listen, nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raftnode: transport: %w", err)
	}
	cfg.LocalID = raft.ServerID(transport.LocalAddr())
	if o.nodeID != "" {
		cfg.LocalID = raft.ServerID(o.nodeID)
	}

	logs, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("raftnode: log store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(dataDir, 2, io.Discard)
	if err != nil {
		logs.Close()
		transport.Close()
		return nil, fmt.Errorf("raftnode: snapshots: %w", err)
	}

	hasState, err := raft.HasExistingState(logs, logs, snaps)
	if err != nil {
		logs.Close()
		transport.Close()
		return nil, err
	}
	r, err := raft.NewRaft(cfg, fsm, logs, logs, snaps, transport)
	if err != nil {
		logs.Close()
		transport.Close()
		return nil, fmt.Errorf("raftnode: new raft: %w", err)
	}
	if !hasState && !o.join {
		f := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			ID:      cfg.LocalID,
			Address: transport.LocalAddr(),
		}}})
		if err := f.Error(); err != nil {
			r.Shutdown()
			logs.Close()
			transport.Close()
			return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
		}
	}
	return &Node{Raft: r, transport: transport, logs: logs, nodeID: string(cfg.LocalID)}, nil
}

// NodeID is this node's cluster identity (its gRPC address).
func (n *Node) NodeID() string { return n.nodeID }

// RaftAddr is the bound raft transport address (meaningful with a :0 listen).
func (n *Node) RaftAddr() string { return string(n.transport.LocalAddr()) }

// Voters lists the current raft configuration — the cluster member table the
// naming sync module builds on. ID is the member's gRPC address.
func (n *Node) Voters() []raft.Server {
	f := n.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil
	}
	return f.Configuration().Servers
}

// WaitLeader blocks until the cluster has a leader — this node right after
// bootstrap, any voter for a joiner — or ctx ends. Callers publish right
// after Start returns.
func (n *Node) WaitLeader(ctx context.Context) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if n.Raft.Leader() != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (n *Node) Close() error {
	err := n.Raft.Shutdown().Error()
	_ = n.transport.Close()
	if cerr := n.logs.Close(); err == nil {
		err = cerr
	}
	return err
}
