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
	flag.StringVar(&opts.GRPCListen, "listen", "127.0.0.1:9848", "gRPC SDK listen address (Nacos convention: main port + 1000)")
	flag.StringVar(&opts.RaftListen, "raft-listen", "127.0.0.1:9849", "raft transport listen address")
	flag.StringVar(&opts.AdminListen, "admin-listen", "127.0.0.1:8848", "read-only console HTTP address (\"off\" disables)")
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
	logger.Info("go-nacos ready", "grpc", a.Addr(), "raft", opts.RaftListen, "admin", a.AdminAddr(), "dataDir", opts.DataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	a.Stop()
}
