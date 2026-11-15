package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/javahongxi/go-nacos/internal/app"
)

func main() {
	var opts app.Options
	flag.StringVar(&opts.GRPCListen, "listen", "127.0.0.1:9848", "gRPC listen address, SDK + internal cluster RPCs (Nacos convention: main port + 1000)")
	flag.StringVar(&opts.RaftListen, "raft-listen", "127.0.0.1:7848", "raft transport listen address (Nacos convention: main port - 1000)")
	flag.StringVar(&opts.JoinAddr, "join", "", "existing node's gRPC address to join (empty = bootstrap a new cluster)")
	flag.StringVar(&opts.AdminListen, "admin-listen", "127.0.0.1:8080", "console HTTP address, Nacos 3.x convention (\"off\" disables)")
	flag.StringVar(&opts.DataDir, "data-dir", "data", "data directory (raft logs, snapshots, bbolt)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	opts.Logger = logger

	a, err := app.New(opts)
	if err != nil {
		logger.Error("startup failed", "err", err)
		os.Exit(1)
	}
	if err := a.Start(); err != nil {
		logger.Error("raft leader wait failed", "err", err)
		a.Stop()
		os.Exit(1)
	}
	logger.Info("go-nacos ready", "grpc", a.Addr(), "raft", opts.RaftListen, "join", opts.JoinAddr, "admin", a.AdminAddr(), "dataDir", opts.DataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-a.Done(): // removed from the cluster: the watcher already self-stopped
	}
	logger.Info("shutting down")
	a.Stop()
}
