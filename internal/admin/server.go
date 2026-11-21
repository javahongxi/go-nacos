// Package admin is the observability console: a small HTTP API over the
// in-process domain services plus an embedded single-page frontend. Reads are
// GET-only; writes are limited to config publish/delete (through raft) and
// naming instance-offline / empty-service delete. It is deliberately separate
// from the gRPC SDK path and does not implement the Nacos 1.x OpenAPI.
package admin

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/javahongxi/go-nacos/internal/cluster"
	"github.com/javahongxi/go-nacos/internal/config"
	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/go-nacos/internal/raftnode"
)

//go:embed static
var staticFS embed.FS

type Server struct {
	cfg     *config.Service
	naming  *naming.Service
	conns   *conn.Registry
	raft    *raftnode.Node
	cluster *cluster.Client
	logger  *slog.Logger
	http    *http.Server
	lis     net.Listener
}

// New wires the routes. Reads are GET-only; the POST/DELETE routes are the
// only mutating entry points. "/" serves the embedded frontend.
func New(cfg *config.Service, naming *naming.Service, conns *conn.Registry, node *raftnode.Node, clusterClient *cluster.Client, lis net.Listener, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, naming: naming, conns: conns, raft: node, cluster: clusterClient, logger: logger, lis: lis}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/overview", s.overview)
	mux.HandleFunc("GET /api/configs", s.configs)
	mux.HandleFunc("GET /api/config", s.configDetail)
	mux.HandleFunc("GET /api/config/history", s.configHistory)
	mux.HandleFunc("GET /api/services", s.services)
	mux.HandleFunc("GET /api/service", s.serviceDetail)
	mux.HandleFunc("GET /api/connections", s.connections)
	mux.HandleFunc("GET /api/raft", s.raftStatus)
	mux.HandleFunc("POST /api/config", s.configPublish)
	mux.HandleFunc("DELETE /api/config", s.configDelete)
	mux.HandleFunc("DELETE /api/instance", s.instanceDelete)
	mux.HandleFunc("DELETE /api/service", s.serviceDelete)
	mux.HandleFunc("DELETE /api/raft/member", s.raftMemberDelete)
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
		"raftLeader":  string(leaderID(s.raft)),
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

// configHistory lists the retained versions of one config, newest first
// (the FSM keeps up to 100 including the current one, r-nacos style).
func (s *Server) configHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	k := config.Key{
		DataID: q.Get("dataId"),
		Group:  orDefault(q.Get("group"), "DEFAULT_GROUP"),
		Tenant: orDefault(q.Get("tenant"), "public"),
	}
	if _, ok := s.cfg.Query(k); !ok {
		http.Error(w, "config not found", http.StatusNotFound)
		return
	}
	hist := s.cfg.History(k)
	for i, j := 0, len(hist)-1; i < j; i, j = i+1, j-1 {
		hist[i], hist[j] = hist[j], hist[i]
	}
	writeJSON(w, map[string]any{
		"dataId": k.DataID, "group": k.Group, "tenant": k.Tenant,
		"total": len(hist), "history": hist,
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
	members := make([]map[string]string, 0)
	for _, sv := range s.raft.Voters() {
		members = append(members, map[string]string{"id": string(sv.ID), "raftAddr": string(sv.Address), "suffrage": sv.Suffrage.String()})
	}
	writeJSON(w, map[string]any{
		"state":   node.State().String(),
		"nodeId":  s.raft.NodeID(),
		"leader":  string(leaderID(s.raft)),
		"members": members,
		"stats":   node.Stats(),
	})
}

// leaderID reports the leader's node identity (its gRPC address), not the
// internal raft transport address, so the console matches join/member IDs.
func leaderID(n *raftnode.Node) string {
	_, id := n.Raft.LeaderWithID()
	return string(id)
}

// configPublish creates or updates a config through raft. A non-empty
// expectMd5 turns it into a CAS publish; a stale md5 loses with 409.
func (s *Server) configPublish(w http.ResponseWriter, r *http.Request) {
	var in struct {
		DataID    string `json:"dataId"`
		Group     string `json:"group"`
		Tenant    string `json:"tenant"`
		Content   string `json:"content"`
		Type      string `json:"type"`
		AppName   string `json:"appName"`
		ExpectMD5 string `json:"expectMd5"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad json body", http.StatusBadRequest)
		return
	}
	if in.DataID == "" {
		http.Error(w, "dataId is required", http.StatusBadRequest)
		return
	}
	k := config.Key{DataID: in.DataID, Group: orDefault(in.Group, "DEFAULT_GROUP"), Tenant: orDefault(in.Tenant, "public")}
	res := s.cfg.Publish(k, in.Content, in.ExpectMD5, in.Type, in.AppName)
	if res.Conflict {
		http.Error(w, res.Message, http.StatusConflict)
		return
	}
	if !res.OK {
		http.Error(w, res.Message, http.StatusInternalServerError)
		return
	}
	it, _ := s.cfg.Query(k)
	s.logger.Info("admin: config published", "dataId", k.DataID, "group", k.Group, "cas", in.ExpectMD5 != "")
	writeJSON(w, map[string]any{"ok": true, "md5": it.MD5})
}

func (s *Server) configDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("dataId") == "" {
		http.Error(w, "dataId is required", http.StatusBadRequest)
		return
	}
	k := config.Key{DataID: q.Get("dataId"), Group: orDefault(q.Get("group"), "DEFAULT_GROUP"), Tenant: orDefault(q.Get("tenant"), "public")}
	res := s.cfg.Remove(k)
	if !res.OK {
		http.Error(w, res.Message, http.StatusInternalServerError)
		return
	}
	s.logger.Info("admin: config removed", "dataId", k.DataID, "group", k.Group)
	writeJSON(w, map[string]any{"ok": true})
}

// instanceDelete force-offlines an instance without touching its connection.
func (s *Server) instanceDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	port, err := strconv.Atoi(q.Get("port"))
	if q.Get("name") == "" || q.Get("ip") == "" || err != nil {
		http.Error(w, "name, ip and a numeric port are required", http.StatusBadRequest)
		return
	}
	k := naming.Key{Namespace: orDefault(q.Get("namespace"), "public"), Group: orDefault(q.Get("group"), "DEFAULT_GROUP"), Name: q.Get("name")}
	if !s.naming.AdminDeregister(k, q.Get("ip"), port, q.Get("cluster")) {
		http.Error(w, "instance not found", http.StatusNotFound)
		return
	}
	s.logger.Info("admin: instance offlined", "service", k.Name, "ip", q.Get("ip"), "port", port)
	writeJSON(w, map[string]any{"ok": true})
}

// serviceDelete drops an empty service shell; the subscriber index survives.
func (s *Server) serviceDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("name") == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	k := naming.Key{Namespace: orDefault(q.Get("namespace"), "public"), Group: orDefault(q.Get("group"), "DEFAULT_GROUP"), Name: q.Get("name")}
	err := s.naming.DeleteService(k)
	switch {
	case errors.Is(err, naming.ErrServiceNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, naming.ErrServiceNotEmpty):
		http.Error(w, err.Error(), http.StatusConflict)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		s.logger.Info("admin: service deleted", "service", k.Name)
		writeJSON(w, map[string]any{"ok": true})
	}
}

// raftMemberDelete removes a voter through the leader, following redirects
// from any node. The removed node self-stops; its naming replicas are swept
// by the member-table diff on every survivor.
func (s *Server) raftMemberDelete(w http.ResponseWriter, r *http.Request) {
	node := r.URL.Query().Get("node")
	if node == "" {
		http.Error(w, "node is required", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	addr := s.raft.NodeID() // leader executes locally; a follower redirects
	var lastMsg string
	for {
		resp, err := s.cluster.Remove(ctx, addr, cluster.RemoveRequest{NodeID: node})
		switch {
		case err == nil && resp.OK:
			s.logger.Info("admin: voter removed", "node", node)
			writeJSON(w, map[string]any{"ok": true})
			return
		case err != nil:
			lastMsg = err.Error()
		default:
			lastMsg = resp.Message
			if resp.LeaderID != "" {
				addr = resp.LeaderID
			}
		}
		if lastMsg == "cannot remove the last voter" {
			http.Error(w, lastMsg, http.StatusConflict)
			return
		}
		select {
		case <-ctx.Done():
			if lastMsg == "" {
				lastMsg = "remove timed out"
			}
			http.Error(w, lastMsg, http.StatusInternalServerError)
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
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
