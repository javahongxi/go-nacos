// Package config is the config domain: every write goes through raft.Apply,
// reads come from the FSM's memory, and change notifications are key-only
// pushes to the connections that listen on the key.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/wire"
)

// Key identifies one config: dataId + group + tenant(namespace).
type Key struct {
	DataID string
	Group  string
	Tenant string
}

func (k Key) String() string { return k.DataID + "\x00" + k.Group + "\x00" + k.Tenant }

const applyTimeout = 5 * time.Second

// Service owns the listen table: groupKey → connID → last md5 the client
// holds. Writes are replicated; listens are connection-local (a reconnect
// re-listens, so they need no replication).
type Service struct {
	r      *raft.Raft
	fsm    *FSM
	conns  *conn.Registry
	logger *slog.Logger

	mu      sync.Mutex
	listens map[string]map[string]string
}

func NewService(r *raft.Raft, fsm *FSM, conns *conn.Registry, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		r:       r,
		fsm:     fsm,
		conns:   conns,
		logger:  logger,
		listens: map[string]map[string]string{},
	}
	conns.OnClose(s.removeConn)
	return s
}

func (s *Service) Query(k Key) (Item, bool) { return s.fsm.Get(k.String()) }

// Publish replicates a write and notifies listeners when the content changed.
func (s *Service) Publish(k Key, content, casMD5, typ, appName string) ApplyResult {
	res := s.apply(Command{
		Op:      "publish",
		Key:     k.String(),
		Content: content,
		CasMD5:  casMD5,
		Type:    typ,
		AppName: appName,
		Now:     time.Now().UnixMilli(),
	})
	if res.OK && res.Changed {
		s.notify(k)
	}
	return res
}

// Remove replicates a delete. Idempotent; always notifies (Nacos fires the
// event on remove regardless of existence).
func (s *Service) Remove(k Key) ApplyResult {
	res := s.apply(Command{Op: "remove", Key: k.String(), Now: time.Now().UnixMilli()})
	if res.OK && res.Changed {
		s.notify(k)
	}
	return res
}

func (s *Service) apply(cmd Command) ApplyResult {
	data, err := json.Marshal(cmd)
	if err != nil {
		return ApplyResult{Message: err.Error()}
	}
	f := s.r.Apply(data, applyTimeout)
	if err := f.Error(); err != nil {
		return ApplyResult{Message: fmt.Sprintf("config: raft apply: %v", err)}
	}
	res, ok := f.Response().(ApplyResult)
	if !ok {
		return ApplyResult{Message: "config: unexpected apply response"}
	}
	return res
}

// Listen registers (or drops) a batch of watched keys for one connection and
// returns the stale subset immediately — md5 equality is string equality, and
// a missing config's md5 is "" (ConfigCacheService.isUptodate).
func (s *Service) Listen(connID string, contexts []wire.ConfigListenContext, listen bool) []wire.ConfigContext {
	var changed []wire.ConfigContext
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lc := range contexts {
		k := Key{DataID: lc.DataID, Group: lc.Group, Tenant: lc.Tenant}
		ks := k.String()
		if !listen {
			if m := s.listens[ks]; m != nil {
				delete(m, connID)
			}
			continue
		}
		m := s.listens[ks]
		if m == nil {
			m = map[string]string{}
			s.listens[ks] = m
		}
		m[connID] = lc.MD5
		serverMD5 := ""
		if it, ok := s.fsm.Get(ks); ok {
			serverMD5 = it.MD5
		}
		if serverMD5 != lc.MD5 {
			changed = append(changed, wire.ConfigContext{Group: k.Group, DataID: k.DataID, Tenant: k.Tenant})
		}
	}
	return changed
}

// notify pushes the key to every listener. No md5 filtering here — Nacos
// pushes to all listeners of the key and lets the client's md5 guard decide
// whether to re-query (RpcConfigChangeNotifier).
func (s *Service) notify(k Key) {
	ks := k.String()
	s.mu.Lock()
	var ids []string
	for id := range s.listens[ks] {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		c := s.conns.GetByID(id)
		if c == nil {
			continue
		}
		go func(c *conn.Connection) {
			reqID := c.NextRequestID()
			p, err := wire.NewPayload(wire.TypeConfigChangeNotifyRequest, wire.ConfigChangeNotifyRequest{
				RequestID: reqID,
				DataID:    k.DataID,
				Group:     k.Group,
				Tenant:    k.Tenant,
			})
			if err != nil {
				return
			}
			if err := s.conns.Push(c, p, reqID); err != nil {
				s.logger.Warn("config notify failed", "conn", c.ID, "dataId", k.DataID, "err", err)
			}
		}(c)
	}
}

// removeConn drops every listen the connection held.
func (s *Service) removeConn(connID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ks, m := range s.listens {
		delete(m, connID)
		if len(m) == 0 {
			delete(s.listens, ks)
		}
	}
}
