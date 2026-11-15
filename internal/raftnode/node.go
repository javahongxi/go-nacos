// Package raftnode assembles hashicorp/raft: one bolt file for log+stable
// storage, file snapshots, TCP transport. M1 bootstraps a single-node cluster
// (restart-safe via HasExistingState); M2 adds join and leader routing.
package raftnode

import (
	"context"
	"fmt"
	"io"
	"net"
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
}

func New(dataDir, listen string, fsm raft.FSM) (*Node, error) {
	cfg := raft.DefaultConfig()
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "gonacos-raft", Level: hclog.Warn, Output: os.Stderr})

	addr, err := net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("raftnode: resolve %s: %w", listen, err)
	}
	transport, err := raft.NewTCPTransport(listen, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raftnode: transport: %w", err)
	}
	// Advertise the actually-bound address, so a :0 listen works in tests.
	cfg.LocalID = raft.ServerID(transport.LocalAddr())

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
	if !hasState {
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
	return &Node{Raft: r, transport: transport, logs: logs}, nil
}

// WaitLeader blocks until this node is leader (single node: after the first
// election, ~1s) or ctx ends. Callers publish right after Start returns.
func (n *Node) WaitLeader(ctx context.Context) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if n.Raft.State() == raft.Leader {
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
