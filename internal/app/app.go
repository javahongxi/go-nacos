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

	"github.com/javahongxi/go-nacos/internal/admin"
	"github.com/javahongxi/go-nacos/internal/cluster"
	"github.com/javahongxi/go-nacos/internal/config"
	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/grpcserver"
	"github.com/javahongxi/go-nacos/internal/naming"
	nsync "github.com/javahongxi/go-nacos/internal/naming/sync"
	"github.com/javahongxi/go-nacos/internal/raftnode"
)

type Options struct {
	GRPCListen  string // gRPC SDK address, default 127.0.0.1:9848
	RaftListen  string // raft transport address, default 127.0.0.1:9849
	SyncListen  string // internal node-to-node address, default 127.0.0.1:9850
	AdminListen string // read-only console address, default 127.0.0.1:8848 ("off" disables)
	JoinAddr    string // existing node's sync address; empty = bootstrap a new cluster
	DataDir     string // default "data"
	Logger      *slog.Logger

	NamingCleanExpire   time.Duration // empty-service expiry, zero = 60s (Java default)
	NamingCleanInterval time.Duration // empty-service scan interval, zero = 60s
}

type App struct {
	Conns   *conn.Registry
	Config  *config.Service
	Naming  *naming.Service
	Raft    *raftnode.Node
	GRPC    *grpcserver.Server
	Admin   *admin.Server
	Cluster *cluster.Server
	Sync    *nsync.Manager

	store         *config.Store
	lis           net.Listener
	syncLis       net.Listener
	clusterClient *cluster.Client
	nodeID        string
	joinAddr      string
	stopOnce      sync.Once
}

func New(opts Options) (*App, error) {
	if opts.GRPCListen == "" {
		opts.GRPCListen = "127.0.0.1:9848"
	}
	if opts.RaftListen == "" {
		opts.RaftListen = "127.0.0.1:9849"
	}
	if opts.SyncListen == "" {
		opts.SyncListen = "127.0.0.1:9850"
	}
	if opts.AdminListen == "" {
		opts.AdminListen = "127.0.0.1:8848"
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

	// The internal listener comes first: its bound address is the node's
	// cluster identity (raft ServerID), so a :0 listen works in tests.
	syncLis, err := net.Listen("tcp", opts.SyncListen)
	if err != nil {
		return nil, fmt.Errorf("app: sync listen %s: %w", opts.SyncListen, err)
	}
	nodeID := syncLis.Addr().String()

	store, err := config.OpenStore(filepath.Join(opts.DataDir, "config.db"))
	if err != nil {
		syncLis.Close()
		return nil, err
	}
	fsm, err := config.NewFSM(store)
	if err != nil {
		store.Close()
		syncLis.Close()
		return nil, err
	}
	raftOpts := []raftnode.Option{raftnode.WithNodeID(nodeID)}
	if opts.JoinAddr != "" {
		raftOpts = append(raftOpts, raftnode.WithJoin())
	}
	node, err := raftnode.New(opts.DataDir, opts.RaftListen, fsm, raftOpts...)
	if err != nil {
		store.Close()
		syncLis.Close()
		return nil, err
	}
	conns := conn.NewRegistry(nodeID, opts.Logger)
	clusterClient := cluster.NewClient(opts.Logger)
	cfg := config.NewService(node.Raft, fsm, conns, opts.Logger, clusterClient)
	namingSvc := naming.NewService(conns, opts.Logger,
		naming.WithCleanerTiming(opts.NamingCleanExpire, opts.NamingCleanInterval),
		naming.WithNodeID(nodeID))
	// The raft member table is the peer list: ServerID IS the sync address.
	peers := func() []string {
		servers := node.Voters()
		out := make([]string, 0, len(servers))
		for _, srv := range servers {
			out = append(out, string(srv.ID))
		}
		return out
	}
	syncMgr := nsync.New(nodeID, namingSvc, clusterClient, peers, opts.Logger)
	namingSvc.SetEmitter(syncMgr)
	clusterSrv := cluster.NewServer(node, cfg, syncMgr, syncLis, opts.Logger)
	srv := grpcserver.New(conns, cfg, namingSvc, opts.Logger)

	lis, err := net.Listen("tcp", opts.GRPCListen)
	if err != nil {
		node.Close()
		store.Close()
		syncLis.Close()
		clusterClient.Close()
		return nil, fmt.Errorf("app: listen %s: %w", opts.GRPCListen, err)
	}
	var adminSrv *admin.Server
	if opts.AdminListen != "off" {
		adminLis, err := net.Listen("tcp", opts.AdminListen)
		if err != nil {
			node.Close()
			store.Close()
			syncLis.Close()
			clusterClient.Close()
			return nil, fmt.Errorf("app: admin listen %s: %w", opts.AdminListen, err)
		}
		adminSrv = admin.New(cfg, namingSvc, conns, node, adminLis, opts.Logger)
	}
	return &App{
		Conns: conns, Config: cfg, Naming: namingSvc, Raft: node, GRPC: srv, Admin: adminSrv, Cluster: clusterSrv, Sync: syncMgr,
		store: store, lis: lis, syncLis: syncLis, clusterClient: clusterClient, nodeID: nodeID, joinAddr: opts.JoinAddr,
	}, nil
}

// Addr is the bound gRPC address (meaningful with a :0 listen).
func (a *App) Addr() string { return a.lis.Addr().String() }

// SyncAddr is the bound internal address — the node's cluster identity and
// the address peers join through.
func (a *App) SyncAddr() string { return a.syncLis.Addr().String() }

// AdminAddr is the bound console address ("" when admin is disabled).
func (a *App) AdminAddr() string {
	if a.Admin == nil {
		return ""
	}
	return a.Admin.Addr()
}

// Start serves in the background, joins the cluster when configured, and
// waits until raft has a leader, so a caller can publish immediately after
// it returns.
func (a *App) Start() error {
	go func() { _ = a.GRPC.Serve(a.lis) }()
	go func() { _ = a.Cluster.Serve() }()
	if a.Admin != nil {
		go func() { _ = a.Admin.Serve() }()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if a.joinAddr != "" {
		if err := a.join(ctx); err != nil {
			return err
		}
	}
	if err := a.Raft.WaitLeader(ctx); err != nil {
		return err
	}
	a.Sync.Start()
	return nil
}

// join retries against the configured peer, following leader redirects,
// until the cluster accepts us as a voter.
func (a *App) join(ctx context.Context) error {
	addr := a.joinAddr
	var lastErr error
	var lastResp cluster.JoinResponse
	for {
		resp, err := a.clusterClient.Join(ctx, addr, cluster.JoinRequest{NodeID: a.nodeID, RaftAddr: a.Raft.RaftAddr()})
		if err == nil && resp.OK {
			return nil
		}
		lastErr, lastResp = err, resp
		if err == nil && resp.LeaderID != "" {
			addr = resp.LeaderID
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("app: join %s: %w (last resp=%+v err=%v)", a.joinAddr, ctx.Err(), lastResp, lastErr)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (a *App) Stop() {
	a.stopOnce.Do(func() { a.shutdown(false) })
}

// HardStop simulates a process crash for e2e: no connection retirement, no
// goodbye broadcasts — peers must detect the death via the ping timeout.
func (a *App) HardStop() {
	a.stopOnce.Do(func() { a.shutdown(true) })
}

func (a *App) shutdown(hard bool) {
	if hard {
		a.Sync.Stop() // silence first: the crash must not broadcast
		a.GRPC.HardStop()
	} else {
		a.GRPC.Stop() // retire conns while sync still runs: peers get the deletes
		a.Sync.Stop()
	}
	a.Naming.Close()
	if a.Admin != nil {
		a.Admin.Close()
	}
	a.Cluster.Stop()
	_ = a.Raft.Close()
	_ = a.store.Close()
	a.clusterClient.Close()
}

// DropConnections forces every client to reconnect; e2e replay tests use it.
func (a *App) DropConnections() { a.GRPC.DropConnections() }
