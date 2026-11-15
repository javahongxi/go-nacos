// Package naming is the naming domain: ephemeral instances live in memory and
// their liveness IS the connection (no beat in the gRPC model). Nothing here
// goes through raft — cluster-wide instance sync is an M2 topic (r-nacos
// splits the same way: config through raft, ephemeral instances not).
package naming

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/wire"
)

// Key identifies one service: namespace + group + name.
type Key struct {
	Namespace string
	Group     string
	Name      string
}

func (k Key) String() string { return k.Namespace + "\x00" + k.Group + "\x00" + k.Name }

type instanceEntry struct {
	inst   wire.Instance
	connID string
}

type serviceState struct {
	key         Key
	instances   map[string]instanceEntry // ip#port#cluster → entry
	subscribers map[string]bool          // connID set
}

// Service owns the instance and subscription tables. Both are keyed by
// connection for cleanup: when a connection retires, everything it registered
// or subscribed goes with it.
type Service struct {
	mu       sync.Mutex
	services map[string]*serviceState
	conns    *conn.Registry
	logger   *slog.Logger
}

func NewService(conns *conn.Registry, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{services: map[string]*serviceState{}, conns: conns, logger: logger}
	conns.OnClose(s.removeConn)
	return s
}

func instanceKey(inst wire.Instance) string {
	return fmt.Sprintf("%s#%d#%s", inst.IP, inst.Port, inst.ClusterName)
}

// Register binds the instance to connID; the same endpoint re-registered by a
// new connection simply takes over the entry.
func (s *Service) Register(k Key, inst wire.Instance, connID string) error {
	if inst.IP == "" || inst.Port <= 0 || inst.Port > 65535 {
		return errors.New("naming: instance requires a valid ip and port")
	}
	if inst.ClusterName == "" {
		inst.ClusterName = "DEFAULT"
	}
	if inst.InstanceID == "" {
		inst.InstanceID = fmt.Sprintf("%s#%d#%s#%s", inst.IP, inst.Port, inst.ClusterName, k.Name)
	}
	s.mu.Lock()
	st := s.stateLocked(k)
	st.instances[instanceKey(inst)] = instanceEntry{inst: inst, connID: connID}
	s.mu.Unlock()
	go s.push(k)
	return nil
}

// Deregister removes the instance only if the caller's connection owns it.
func (s *Service) Deregister(k Key, inst wire.Instance, connID string) {
	if inst.ClusterName == "" {
		inst.ClusterName = "DEFAULT"
	}
	s.mu.Lock()
	st := s.services[k.String()]
	changed := false
	if st != nil {
		ik := instanceKey(inst)
		if e, ok := st.instances[ik]; ok && e.connID == connID {
			delete(st.instances, ik)
			changed = true
		}
	}
	s.mu.Unlock()
	if changed {
		go s.push(k)
	}
}

func (s *Service) Query(k Key, cluster string, healthyOnly bool) wire.ServiceInfo {
	return s.snapshot(k, cluster, healthyOnly)
}

// Subscribe turns pushes on/off for one connection and returns the current
// snapshot, so a subscriber never needs a separate query.
func (s *Service) Subscribe(k Key, connID string, subscribe bool) wire.ServiceInfo {
	s.mu.Lock()
	st := s.stateLocked(k)
	if subscribe {
		st.subscribers[connID] = true
	} else {
		delete(st.subscribers, connID)
	}
	s.mu.Unlock()
	return s.snapshot(k, "", false)
}

func (s *Service) stateLocked(k Key) *serviceState {
	st := s.services[k.String()]
	if st == nil {
		st = &serviceState{
			key:         k,
			instances:   map[string]instanceEntry{},
			subscribers: map[string]bool{},
		}
		s.services[k.String()] = st
	}
	return st
}

// removeConn drops everything the connection owned. The connID guard matters:
// a reconnect replay may have re-registered the same endpoint under the new
// connection before the old one's cleanup runs.
func (s *Service) removeConn(connID string) {
	s.mu.Lock()
	changed := map[string]Key{}
	for _, st := range s.services {
		delete(st.subscribers, connID)
		for ik, e := range st.instances {
			if e.connID == connID {
				delete(st.instances, ik)
				changed[st.key.String()] = st.key
			}
		}
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

func (s *Service) snapshot(k Key, cluster string, healthyOnly bool) wire.ServiceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := wire.ServiceInfo{
		Name:        qualifiedName(k.Name, k.Group),
		GroupName:   k.Group,
		CacheMillis: 10000,
		LastRefTime: time.Now().UnixMilli(),
		Hosts:       []wire.Instance{},
	}
	st := s.services[k.String()]
	if st == nil {
		return info
	}
	for _, e := range st.instances {
		if cluster != "" && e.inst.ClusterName != cluster {
			continue
		}
		if healthyOnly && !e.inst.Healthy {
			continue
		}
		info.Hosts = append(info.Hosts, e.inst)
	}
	sort.Slice(info.Hosts, func(i, j int) bool {
		if info.Hosts[i].IP != info.Hosts[j].IP {
			return info.Hosts[i].IP < info.Hosts[j].IP
		}
		return info.Hosts[i].Port < info.Hosts[j].Port
	})
	return info
}

// qualifiedName mirrors NamingUtils.getGroupedName: "GROUP@@service" unless
// the group is DEFAULT_GROUP.
func qualifiedName(name, group string) string {
	if group == "" || group == "DEFAULT_GROUP" {
		return name
	}
	return group + "@@" + name
}

// push fans a fresh snapshot out to the service's subscribers, each push
// acked independently (a dead subscriber retires via the registry).
func (s *Service) push(k Key) {
	info := s.snapshot(k, "", false)
	s.mu.Lock()
	st := s.services[k.String()]
	var ids []string
	if st != nil {
		for id := range st.subscribers {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	for _, id := range ids {
		c := s.conns.GetByID(id)
		if c == nil {
			continue
		}
		go func(c *conn.Connection) {
			reqID := c.NextRequestID()
			p, err := wire.NewPayload(wire.TypeNotifySubscriberRequest, wire.NotifySubscriberRequest{
				RequestID:   reqID,
				ServiceInfo: info,
			})
			if err != nil {
				return
			}
			if err := s.conns.Push(c, p, reqID); err != nil {
				s.logger.Warn("naming push failed", "conn", c.ID, "service", k.Name, "err", err)
			}
		}(c)
	}
}
