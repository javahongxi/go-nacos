// Package app assembles the server: stores, raft, connection registry, domain
// services, gRPC. Both cmd/gonacos and the e2e tests build through here.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/javahongxi/go-nacos/internal/config"
	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/grpcserver"
	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/go-nacos/internal/raftnode"
)

type Options struct {
	GRPCListen string // gRPC SDK address, default 127.0.0.1:9848
	RaftListen string // raft transport address, default 127.0.0.1:9849
	DataDir    string // default "data"
	Logger     *slog.Logger
}

type App struct {
	Conns  *conn.Registry
	Config *config.Service
	Naming *naming.Service
	Raft   *raftnode.Node
	GRPC   *grpcserver.Server

	store    *config.Store
	lis      net.Listener
	stopOnce sync.Once
}

func New(opts Options) (*App, error) {
	if opts.GRPCListen == "" {
		opts.GRPCListen = "127.0.0.1:9848"
	}
	if opts.RaftListen == "" {
		opts.RaftListen = "127.0.0.1:9849"
	}
	if opts.DataDir == "" {
		opts.DataDir = "data"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("app: data dir: %w", err)
	}

	store, err := config.OpenStore(filepath.Join(opts.DataDir, "config.db"))
	if err != nil {
		return nil, err
	}
	fsm, err := config.NewFSM(store)
	if err != nil {
		store.Close()
		return nil, err
	}
	node, err := raftnode.New(opts.DataDir, opts.RaftListen, fsm)
	if err != nil {
		store.Close()
		return nil, err
	}
	conns := conn.NewRegistry(opts.Logger)
	cfg := config.NewService(node.Raft, fsm, conns, opts.Logger)
	namingSvc := naming.NewService(conns, opts.Logger)
	srv := grpcserver.New(conns, cfg, namingSvc, opts.Logger)

	lis, err := net.Listen("tcp", opts.GRPCListen)
	if err != nil {
		node.Close()
		store.Close()
		return nil, fmt.Errorf("app: listen %s: %w", opts.GRPCListen, err)
	}
	return &App{Conns: conns, Config: cfg, Naming: namingSvc, Raft: node, GRPC: srv, store: store, lis: lis}, nil
}

// Addr is the bound gRPC address (meaningful with a :0 listen).
func (a *App) Addr() string { return a.lis.Addr().String() }

// Start serves in the background and waits until raft has a leader, so a
// caller can publish immediately after it returns.
func (a *App) Start() error {
	go func() { _ = a.GRPC.Serve(a.lis) }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return a.Raft.WaitLeader(ctx)
}

func (a *App) Stop() {
	a.stopOnce.Do(func() {
		a.GRPC.Stop()
		_ = a.Raft.Close()
		_ = a.store.Close()
	})
}

// DropConnections forces every client to reconnect; e2e replay tests use it.
func (a *App) DropConnections() { a.GRPC.DropConnections() }
