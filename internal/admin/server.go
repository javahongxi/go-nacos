// Package admin is a read-only observability console: a small HTTP API over
// the in-process domain services plus an embedded single-page frontend. It is
// deliberately separate from the gRPC SDK path — no writes, no Nacos 1.x
// OpenAPI, just inspection of config / naming / connections / raft.
package admin

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/javahongxi/go-nacos/internal/config"
	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/go-nacos/internal/raftnode"
)

//go:embed static
var staticFS embed.FS

type Server struct {
	cfg    *config.Service
	naming *naming.Service
	conns  *conn.Registry
	raft   *raftnode.Node
	logger *slog.Logger
	http   *http.Server
	lis    net.Listener
}

// New wires the read-only routes. Every API route is GET-only (the mux returns
// 405 for anything else); "/" serves the embedded frontend.
func New(cfg *config.Service, naming *naming.Service, conns *conn.Registry, node *raftnode.Node, lis net.Listener, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, naming: naming, conns: conns, raft: node, logger: logger, lis: lis}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/overview", s.overview)
	mux.HandleFunc("GET /api/configs", s.configs)
	mux.HandleFunc("GET /api/config", s.configDetail)
	mux.HandleFunc("GET /api/services", s.services)
	mux.HandleFunc("GET /api/service", s.serviceDetail)
	mux.HandleFunc("GET /api/connections", s.connections)
	mux.HandleFunc("GET /api/raft", s.raftStatus)
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err) // embedded dir is guaranteed by go:embed
	}
	mux.Handle("GET /", http.FileServer(http.FS(sub)))

	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) Serve() error { return s.http.Serve(s.lis) }

// Addr is the bound admin address (meaningful with a :0 listen).
func (s *Server) Addr() string { return s.lis.Addr().String() }

func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.http.Shutdown(ctx)
}

func (s *Server) overview(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"configs":     len(s.cfg.List()),
		"services":    len(s.naming.List()),
		"connections": len(s.conns.List()),
		"raftState":   s.raft.Raft.State().String(),
		"raftLeader":  string(s.raft.Raft.Leader()),
	})
}

func (s *Server) configs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.List())
}

func (s *Server) configDetail(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	k := config.Key{
		DataID: q.Get("dataId"),
		Group:  orDefault(q.Get("group"), "DEFAULT_GROUP"),
		Tenant: orDefault(q.Get("tenant"), "public"),
	}
	it, ok := s.cfg.Query(k)
	if !ok {
		http.Error(w, "config not found", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]any{
		"dataId": k.DataID, "group": k.Group, "tenant": k.Tenant,
		"content": it.Content, "md5": it.MD5, "type": it.Type,
		"appName": it.AppName, "lastModified": it.LastModified,
	})
}

func (s *Server) services(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.naming.List())
}

func (s *Server) serviceDetail(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	k := naming.Key{
		Namespace: orDefault(q.Get("namespace"), "public"),
		Group:     orDefault(q.Get("group"), "DEFAULT_GROUP"),
		Name:      q.Get("name"),
	}
	writeJSON(w, map[string]any{
		"serviceInfo": s.naming.Query(k, "", false),
		"subscribers": s.naming.Subscribers(k),
	})
}

func (s *Server) connections(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.conns.List())
}

func (s *Server) raftStatus(w http.ResponseWriter, _ *http.Request) {
	node := s.raft.Raft
	writeJSON(w, map[string]any{
		"state":  node.State().String(),
		"leader": string(node.Leader()),
		"stats":  node.Stats(),
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
