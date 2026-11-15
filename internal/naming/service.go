// Package naming is the naming domain: ephemeral instances live in memory and
// their liveness IS the connection (no beat in the gRPC model). Nothing here
// goes through raft — cluster-wide instance sync is an M2 topic (r-nacos
// splits the same way: config through raft, ephemeral instances not).
//
// Publisher and subscriber indexes are kept apart, mirroring Java's
// ClientServiceIndexesManager: the empty-service cleaner drops instance-less
// service shells without touching subscriptions, so pushes survive cleanup.
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
	lastUpdated int64                    // UnixMilli of the last instance change
}

// Service owns the instance shells and an independent subscriber index
// (serviceKey → connID set), mirroring Java's publisherIndexes /
// subscriberIndexes split. When a connection retires, everything it registered
// or subscribed goes with it; the empty-service cleaner only drops shells.
type Service struct {
	mu          sync.Mutex
	services    map[string]*serviceState
	subscribers map[string]map[string]bool
	conns       *conn.Registry
	logger      *slog.Logger

	cleanExpire time.Duration
	cleanEvery  time.Duration
	stopClean   chan struct{}
	cleanDone   chan struct{}
	closeOnce   sync.Once
}

// Option tunes a Service at construction.
type Option func(*Service)

// WithCleanerTiming overrides the empty-service cleaner's expiry and scan
// interval (Java defaults: 60s each). Non-positive values keep the default.
func WithCleanerTiming(expire, interval time.Duration) Option {
	return func(s *Service) {
		if expire > 0 {
			s.cleanExpire = expire
		}
		if interval > 0 {
			s.cleanEvery = interval
		}
	}
}

func NewService(conns *conn.Registry, logger *slog.Logger, opts ...Option) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		services:    map[string]*serviceState{},
		subscribers: map[string]map[string]bool{},
		conns:       conns,
		logger:      logger,
		cleanExpire: 60 * time.Second,
		cleanEvery:  60 * time.Second,
		stopClean:   make(chan struct{}),
		cleanDone:   make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	conns.OnClose(s.removeConn)
	go s.cleanLoop()
	return s
}

// Close stops the empty-service cleaner.
func (s *Service) Close() {
	s.closeOnce.Do(func() {
		close(s.stopClean)
		<-s.cleanDone
	})
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
	st.lastUpdated = time.Now().UnixMilli()
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
			st.lastUpdated = time.Now().UnixMilli()
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
	ks := k.String()
	s.mu.Lock()
	if subscribe {
		m := s.subscribers[ks]
		if m == nil {
			m = map[string]bool{}
			s.subscribers[ks] = m
		}
		m[connID] = true
		s.stateLocked(k) // keep the shell visible to List while subscribed
	} else if m := s.subscribers[ks]; m != nil {
		delete(m, connID)
		if len(m) == 0 {
			delete(s.subscribers, ks)
		}
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
			lastUpdated: time.Now().UnixMilli(),
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
	now := time.Now().UnixMilli()
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if e.connID == connID {
				delete(st.instances, ik)
				st.lastUpdated = now
				changed[ks] = st.key
			}
		}
	}
	// Java drops the subscriber index on client release too.
	for ks, m := range s.subscribers {
		delete(m, connID)
		if len(m) == 0 {
			delete(s.subscribers, ks)
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
	var ids []string
	for id := range s.subscribers[k.String()] {
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

// cleanLoop scans for empty service shells on a fixed interval, mirroring
// EmptyServiceAutoCleanerV2 (Java: 60s expiry + 60s scan). Only the shell is
// dropped — the subscriber index survives, so a later registration still
// pushes to the original subscribers without any client reconnect.
func (s *Service) cleanLoop() {
	defer close(s.cleanDone)
	t := time.NewTicker(s.cleanEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stopClean:
			return
		case <-t.C:
			s.cleanEmptyServices()
		}
	}
}

// cleanEmptyServices drops shells with no instances whose last instance
// change is older than the expiry. lastUpdated tracks instance changes only
// (Java: ServiceChangedEvent → Service.renewUpdateTime; subscribes don't
// renew it).
func (s *Service) cleanEmptyServices() {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	for ks, st := range s.services {
		if len(st.instances) == 0 && now-st.lastUpdated >= s.cleanExpire.Milliseconds() {
			delete(s.services, ks)
			s.logger.Info("naming: empty service cleaned",
				"namespace", st.key.Namespace, "group", st.key.Group, "service", st.key.Name)
		}
	}
}

// ServiceSummary is a read-only service row for the admin console.
type ServiceSummary struct {
	Namespace   string `json:"namespace"`
	Group       string `json:"group"`
	Name        string `json:"name"`
	Instances   int    `json:"instances"`
	Subscribers int    `json:"subscribers"`
}

// List returns every known service with instance/subscriber counts. Read-only.
func (s *Service) List() []ServiceSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ServiceSummary, 0, len(s.services))
	for ks, st := range s.services {
		out = append(out, ServiceSummary{
			Namespace: st.key.Namespace, Group: st.key.Group, Name: st.key.Name,
			Instances: len(st.instances), Subscribers: len(s.subscribers[ks]),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}

// Subscribers returns the connection ids subscribed to a service. Read-only.
func (s *Service) Subscribers(k Key) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.subscribers[k.String()]
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
