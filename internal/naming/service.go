// Package naming is the naming domain, split the r-nacos way: ephemeral
// instances live in memory and their liveness IS the connection (no beat in
// the gRPC model), replicated by connection-level AP sync (the sync
// subpackage); persistent instances (ephemeral=false) go through raft —
// register/remove are CP writes that survive restarts and node death, while
// their health is probed locally on every node (TCP dial, r-nacos sniffing:
// probe results never ride raft, they only update local state and push).
//
// Publisher and subscriber indexes are kept apart, mirroring Java's
// ClientServiceIndexesManager: the empty-service cleaner drops instance-less
// service shells without touching subscriptions, so pushes survive cleanup.
package naming

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/javahongxi/go-nacos/internal/conn"
	"github.com/javahongxi/go-nacos/internal/fsm"
	"github.com/javahongxi/go-nacos/internal/wire"
)

// Key identifies one service: namespace + group + name.
type Key struct {
	Namespace string `json:"namespace"`
	Group     string `json:"group"`
	Name      string `json:"name"`
}

func (k Key) String() string { return k.Namespace + "\x00" + k.Group + "\x00" + k.Name }

type instanceEntry struct {
	inst       wire.Instance
	connID     string // "" for persistent instances: no connection owns them
	originNode string // node the owning connection lives on ("" if persistent)
}

// Command is one replicated persistent-instance write (fsm.DomainNaming).
// The origin stamps Now so every replica applies identical state.
type Command struct {
	Op   string        `json:"op"` // "register" | "remove"
	Key  Key           `json:"key"`
	Inst wire.Instance `json:"inst"` // remove targets ip#port#cluster only
	Now  int64         `json:"now"`
}

// ApplyResult reports what a persistent-instance Apply did.
type ApplyResult struct {
	OK      bool
	Message string
}

// Entry is one instance with ownership — the unit of cluster sync.
type Entry struct {
	Key        Key           `json:"key"`
	Inst       wire.Instance `json:"inst"`
	ConnID     string        `json:"connId"`
	OriginNode string        `json:"originNode"`
}

// Emitter broadcasts local mutations to peers (implemented by the sync
// manager). Calls must not block: replication is best-effort and the
// periodic full mapping heals whatever slips through.
type Emitter interface {
	EmitRegister(k Key, inst wire.Instance, connID string)
	EmitDeregister(k Key, inst wire.Instance, connID string)
	EmitRemoveConn(connID string)
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
	nodeID      string
	emitter     Emitter
	writer      *fsm.Writer // persistent-instance raft path (SetRaftWriter)

	cleanExpire time.Duration
	cleanEvery  time.Duration
	stopClean   chan struct{}
	cleanDone   chan struct{}

	probeEvery   time.Duration // persistent-instance TCP probe cadence
	probeTimeout time.Duration
	stopProbe    chan struct{}
	probeDone    chan struct{}
	closeOnce    sync.Once
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

// WithNodeID stamps local entries with the owning node — the attribution
// cluster sync guards and death cleanup key on.
func WithNodeID(id string) Option {
	return func(s *Service) { s.nodeID = id }
}

// WithProbeTiming overrides the persistent-instance probe cadence and dial
// timeout (r-nacos defaults: 60s / 1s). Non-positive values keep the default.
func WithProbeTiming(interval, timeout time.Duration) Option {
	return func(s *Service) {
		if interval > 0 {
			s.probeEvery = interval
		}
		if timeout > 0 {
			s.probeTimeout = timeout
		}
	}
}

func NewService(conns *conn.Registry, logger *slog.Logger, opts ...Option) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		services:     map[string]*serviceState{},
		subscribers:  map[string]map[string]bool{},
		conns:        conns,
		logger:       logger,
		cleanExpire:  60 * time.Second,
		cleanEvery:   60 * time.Second,
		stopClean:    make(chan struct{}),
		cleanDone:    make(chan struct{}),
		probeEvery:   60 * time.Second,
		probeTimeout: time.Second,
		stopProbe:    make(chan struct{}),
		probeDone:    make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	conns.OnClose(s.removeConn)
	go s.cleanLoop()
	go s.probeLoop()
	return s
}

// Close stops the empty-service cleaner and the persistent-instance prober.
func (s *Service) Close() {
	s.closeOnce.Do(func() {
		close(s.stopClean)
		close(s.stopProbe)
		<-s.cleanDone
		<-s.probeDone
	})
}

// SetEmitter installs the cluster broadcast hook. Called once during wiring,
// before traffic. Remote-apply paths never emit: loop prevention by
// construction, no from_sync flag needed on the wire.
func (s *Service) SetEmitter(e Emitter) { s.emitter = e }

// SetRaftWriter installs the persistent-instance raft path. Called once
// during wiring (the raft node needs this Service as its naming domain, so
// the writer can only exist after construction).
func (s *Service) SetRaftWriter(w *fsm.Writer) { s.writer = w }

func instanceKey(inst wire.Instance) string {
	return fmt.Sprintf("%s#%d#%s", inst.IP, inst.Port, inst.ClusterName)
}

// Register binds an ephemeral instance to connID; the same endpoint
// re-registered by a new connection simply takes over the entry. A
// persistent instance (ephemeral=false) instead goes through raft: no
// connection owns it, and every node applies the same entry.
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
	if !inst.Ephemeral {
		return s.applyPersistent(Command{Op: "register", Key: k, Inst: inst, Now: time.Now().UnixMilli()})
	}
	s.mu.Lock()
	st := s.stateLocked(k)
	ik := instanceKey(inst)
	if e, ok := st.instances[ik]; ok && !e.inst.Ephemeral {
		s.mu.Unlock()
		// Persistent entries are raft-owned; an ephemeral registration must
		// not steal the endpoint (r-nacos keeps the two kinds apart too).
		return errors.New("naming: endpoint is registered as a persistent instance")
	}
	st.instances[ik] = instanceEntry{inst: inst, connID: connID, originNode: s.nodeID}
	st.lastUpdated = time.Now().UnixMilli()
	s.mu.Unlock()
	if s.emitter != nil {
		s.emitter.EmitRegister(k, inst, connID)
	}
	go s.push(k)
	return nil
}

// applyPersistent replicates one persistent-instance write and waits for the
// commit (CP): the answer returns only after every voter applied the entry.
func (s *Service) applyPersistent(cmd Command) error {
	if s.writer == nil {
		return errors.New("naming: raft writer unavailable")
	}
	res, err := fsm.Apply[ApplyResult](s.writer, fsm.DomainNaming, cmd)
	if err != nil {
		return fmt.Errorf("naming: raft apply: %w", err)
	}
	if !res.OK {
		return errors.New(res.Message)
	}
	return nil
}

// Deregister removes an ephemeral instance only if the caller's connection
// owns it; a persistent instance goes through raft with no ownership guard.
func (s *Service) Deregister(k Key, inst wire.Instance, connID string) {
	if inst.ClusterName == "" {
		inst.ClusterName = "DEFAULT"
	}
	if !inst.Ephemeral {
		if err := s.applyPersistent(Command{Op: "remove", Key: k, Inst: inst, Now: time.Now().UnixMilli()}); err != nil {
			s.logger.Warn("naming: persistent deregister failed", "service", k.Name, "err", err)
		}
		return
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
		if s.emitter != nil {
			s.emitter.EmitDeregister(k, inst, connID)
		}
		go s.push(k)
	}
}

// Errors returned by DeleteService.
var (
	ErrServiceNotFound = errors.New("naming: service not found")
	ErrServiceNotEmpty = errors.New("naming: service still has instances")
)

// AdminDeregister force-removes an instance regardless of which connection
// owns it — the console override. A persistent target goes through raft (the
// removal replicates cluster-wide); an ephemeral one is broadcast with its
// captured owner conn. The owning client is NOT disconnected; it simply
// stops being listed until it re-registers (e.g. on reconnect replay).
func (s *Service) AdminDeregister(k Key, ip string, port int, cluster string) bool {
	if cluster == "" {
		cluster = "DEFAULT"
	}
	ik := fmt.Sprintf("%s#%d#%s", ip, port, cluster)
	s.mu.Lock()
	st := s.services[k.String()]
	persistent := false
	if st != nil {
		if e, ok := st.instances[ik]; ok && !e.inst.Ephemeral {
			persistent = true
		}
	}
	s.mu.Unlock()
	if persistent {
		inst := wire.Instance{IP: ip, Port: port, ClusterName: cluster}
		if err := s.applyPersistent(Command{Op: "remove", Key: k, Inst: inst, Now: time.Now().UnixMilli()}); err != nil {
			s.logger.Warn("naming: persistent admin deregister failed", "service", k.Name, "err", err)
			return false
		}
		return true
	}
	s.mu.Lock()
	st = s.services[k.String()]
	removed := false
	var owned instanceEntry
	if st != nil {
		if e, ok := st.instances[ik]; ok && e.inst.Ephemeral {
			owned = e
			delete(st.instances, ik)
			st.lastUpdated = time.Now().UnixMilli()
			removed = true
		}
	}
	s.mu.Unlock()
	if removed {
		// Broadcast with the captured owner conn: the connID guard is
		// cluster-unique, so the override applies on every node — even when
		// the console sits on a node that only holds a replica.
		if s.emitter != nil {
			s.emitter.EmitDeregister(k, owned.inst, owned.connID)
		}
		go s.push(k)
	}
	return removed
}

// DeleteService drops an empty service shell on demand (console override of
// the cleaner). Like the cleaner, it leaves the subscriber index intact, so a
// later registration still pushes to the original subscribers.
func (s *Service) DeleteService(k Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.services[k.String()]
	if st == nil {
		return ErrServiceNotFound
	}
	if len(st.instances) > 0 {
		return ErrServiceNotEmpty
	}
	delete(s.services, k.String())
	return nil
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
// connection before the old one's cleanup runs. Persistent instances have no
// owning connection and survive every disconnect.
func (s *Service) removeConn(connID string) {
	s.mu.Lock()
	changed := map[string]Key{}
	now := time.Now().UnixMilli()
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if e.inst.Ephemeral && e.connID == connID {
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
	if len(changed) > 0 && s.emitter != nil {
		s.emitter.EmitRemoveConn(connID)
	}
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

// --- cluster sync: remote-apply paths (never emit, so no sync loops) ---
//
// Every path here is ephemeral-only: persistent instances are raft-owned,
// replicated by the log itself, and must never be touched by AP messages
// (both directions: incoming persistent claims are ignored, local persistent
// entries are never deleted or overwritten).

// ApplyRemoteRegister applies a peer's registration. A live local entry wins
// over a remote claim: a stale broadcast must not steal an endpoint a local
// connection owns; reconciliation re-converges the loser.
func (s *Service) ApplyRemoteRegister(from string, k Key, inst wire.Instance, connID string) {
	if !inst.Ephemeral {
		return
	}
	if inst.ClusterName == "" {
		inst.ClusterName = "DEFAULT"
	}
	s.mu.Lock()
	st := s.stateLocked(k)
	ik := instanceKey(inst)
	if e, ok := st.instances[ik]; ok {
		if !e.inst.Ephemeral || e.originNode == s.nodeID || (e.connID == connID && e.originNode == from && sameInstance(e.inst, inst)) {
			s.mu.Unlock()
			return
		}
	}
	st.instances[ik] = instanceEntry{inst: inst, connID: connID, originNode: from}
	st.lastUpdated = time.Now().UnixMilli()
	s.mu.Unlock()
	go s.push(k)
}

// ApplyRemoteDeregister guards on connID alone: ids are cluster-unique, so
// the guard is safe regardless of which node broadcasts (admin offline may
// originate on a replica holder) and never deletes a newer connection's
// re-registration of the same endpoint.
func (s *Service) ApplyRemoteDeregister(_ string, k Key, inst wire.Instance, connID string) {
	if !inst.Ephemeral {
		return
	}
	if inst.ClusterName == "" {
		inst.ClusterName = "DEFAULT"
	}
	s.mu.Lock()
	st := s.services[k.String()]
	changed := false
	if st != nil {
		ik := instanceKey(inst)
		if e, ok := st.instances[ik]; ok && e.inst.Ephemeral && e.connID == connID {
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

// ApplyRemoteRemoveConn drops a peer's retired connection: both guards, so a
// sender can only purge its own connections' entries.
func (s *Service) ApplyRemoteRemoveConn(from, connID string) {
	s.mu.Lock()
	changed := map[string]Key{}
	now := time.Now().UnixMilli()
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if e.inst.Ephemeral && e.connID == connID && e.originNode == from {
				delete(st.instances, ik)
				st.lastUpdated = now
				changed[ks] = st.key
			}
		}
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

// Reconcile applies a peer's full connID → instances mapping: delete replicas
// the peer no longer claims, upsert claimed entries we missed. Only entries
// with originNode == from are touched, and local takeovers are never
// overwritten — the two guards that keep reconciliation from eating live
// state. This heals lost increments in BOTH directions.
func (s *Service) Reconcile(from string, conns map[string][]Entry) {
	claimed := map[string]map[string]bool{}
	for connID, entries := range conns {
		m := map[string]bool{}
		for _, e := range entries {
			m[e.Key.String()+"\x00"+instanceKey(e.Inst)] = true
		}
		claimed[connID] = m
	}
	s.mu.Lock()
	changed := map[string]Key{}
	now := time.Now().UnixMilli()
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if !e.inst.Ephemeral || e.originNode != from {
				continue
			}
			if !claimed[e.connID][ks+"\x00"+ik] {
				delete(st.instances, ik)
				st.lastUpdated = now
				changed[ks] = st.key
			}
		}
	}
	for connID, entries := range conns {
		for _, e := range entries {
			inst := e.Inst
			if !inst.Ephemeral {
				continue
			}
			if inst.ClusterName == "" {
				inst.ClusterName = "DEFAULT"
			}
			st := s.stateLocked(e.Key)
			ik := instanceKey(inst)
			if cur, ok := st.instances[ik]; ok {
				if !cur.inst.Ephemeral || cur.originNode == s.nodeID || cur.connID != connID {
					continue // raft-owned, local takeover, or a newer conn owns it
				}
				if cur.originNode == from && sameInstance(cur.inst, inst) {
					continue // unchanged: no push noise every cycle
				}
			}
			st.instances[ik] = instanceEntry{inst: inst, connID: connID, originNode: from}
			st.lastUpdated = now
			changed[e.Key.String()] = e.Key
		}
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

// PurgeNode drops every ephemeral instance owned by a dead node (ping
// timeout). Persistent instances survive node death — that is their point.
func (s *Service) PurgeNode(from string) {
	s.mu.Lock()
	changed := map[string]Key{}
	now := time.Now().UnixMilli()
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if e.inst.Ephemeral && e.originNode == from {
				delete(st.instances, ik)
				st.lastUpdated = now
				changed[ks] = st.key
			}
		}
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

// PurgeAbsentNodes drops replicas whose origin node left the raft member
// table (a committed removal). The keep set includes the caller itself, so
// local entries are exempt. Like PurgeNode this never emits: every node
// sweeps its own replicas independently, and a missed sweep heals next tick.
func (s *Service) PurgeAbsentNodes(keep map[string]bool) {
	s.mu.Lock()
	changed := map[string]Key{}
	now := time.Now().UnixMilli()
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if e.inst.Ephemeral && !keep[e.originNode] {
				delete(st.instances, ik)
				st.lastUpdated = now
				changed[ks] = st.key
			}
		}
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

// RestoreSnapshot applies a full-state pull, keeping each entry's true
// origin so death cleanup stays correct on the puller too. Our own entries
// never come back stale, and local takeovers are never overwritten.
func (s *Service) RestoreSnapshot(entries []Entry) {
	s.mu.Lock()
	changed := map[string]Key{}
	now := time.Now().UnixMilli()
	for _, e := range entries {
		if !e.Inst.Ephemeral || e.OriginNode == s.nodeID {
			continue
		}
		st := s.stateLocked(e.Key)
		ik := instanceKey(e.Inst)
		if cur, ok := st.instances[ik]; ok && (!cur.inst.Ephemeral || cur.originNode == s.nodeID || cur.connID != e.ConnID) {
			continue
		}
		st.instances[ik] = instanceEntry{inst: e.Inst, connID: e.ConnID, originNode: e.OriginNode}
		st.lastUpdated = now
		changed[e.Key.String()] = e.Key
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

// AllEntries dumps the full global view (snapshot pulls).
func (s *Service) AllEntries() []Entry {
	return s.entries(false)
}

// LocalEntries dumps only this node's own entries (full-mapping broadcasts).
func (s *Service) LocalEntries() []Entry {
	return s.entries(true)
}

func (s *Service) entries(localOnly bool) []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entry
	for _, st := range s.services {
		for _, e := range st.instances {
			if !e.inst.Ephemeral {
				continue // persistent state rides raft, never the AP channel
			}
			if localOnly && e.originNode != s.nodeID {
				continue
			}
			out = append(out, Entry{Key: st.key, Inst: e.inst, ConnID: e.connID, OriginNode: e.originNode})
		}
	}
	return out
}

func sameInstance(a, b wire.Instance) bool {
	return a.IP == b.IP && a.Port == b.Port && a.ClusterName == b.ClusterName &&
		a.Weight == b.Weight && a.Healthy == b.Healthy && a.Enabled == b.Enabled &&
		a.Ephemeral == b.Ephemeral && a.InstanceID == b.InstanceID && maps.Equal(a.Metadata, b.Metadata)
}

// --- persistent instances: the raft domain (fsm.DomainNaming) ---

// ApplyData applies one replicated persistent-instance command on EVERY
// node: the entry carries no connection ownership, and the change push fires
// locally just like the config FSM hook.
func (s *Service) ApplyData(data []byte) any {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return ApplyResult{Message: fmt.Sprintf("naming: undecodable command: %v", err)}
	}
	switch cmd.Op {
	case "register":
		inst := cmd.Inst
		if inst.ClusterName == "" {
			inst.ClusterName = "DEFAULT"
		}
		inst.Ephemeral = false
		s.mu.Lock()
		st := s.stateLocked(cmd.Key)
		st.instances[instanceKey(inst)] = instanceEntry{inst: inst}
		st.lastUpdated = cmd.Now
		s.mu.Unlock()
		go s.push(cmd.Key)
		return ApplyResult{OK: true}

	case "remove":
		inst := cmd.Inst
		if inst.ClusterName == "" {
			inst.ClusterName = "DEFAULT"
		}
		s.mu.Lock()
		st := s.services[cmd.Key.String()]
		changed := false
		if st != nil {
			ik := instanceKey(inst)
			if e, ok := st.instances[ik]; ok && !e.inst.Ephemeral {
				delete(st.instances, ik)
				st.lastUpdated = cmd.Now
				changed = true
			}
		}
		s.mu.Unlock()
		if changed {
			go s.push(cmd.Key)
		}
		return ApplyResult{OK: true} // idempotent

	default:
		return ApplyResult{Message: fmt.Sprintf("naming: unknown op %q", cmd.Op)}
	}
}

// SnapshotData exports every persistent instance for the composite raft
// snapshot (ephemeral state never rides raft).
func (s *Service) SnapshotData() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entry
	for _, st := range s.services {
		for _, e := range st.instances {
			if !e.inst.Ephemeral {
				out = append(out, Entry{Key: st.key, Inst: e.inst})
			}
		}
	}
	return out
}

// RestoreData swaps in the snapshot's persistent instances, leaving any
// ephemeral state untouched (restore runs at startup, before traffic).
func (s *Service) RestoreData(data []byte) error {
	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.services {
		for ik, e := range st.instances {
			if !e.inst.Ephemeral {
				delete(st.instances, ik)
			}
		}
	}
	for _, e := range entries {
		inst := e.Inst
		if inst.ClusterName == "" {
			inst.ClusterName = "DEFAULT"
		}
		inst.Ephemeral = false
		st := s.stateLocked(e.Key)
		st.instances[instanceKey(inst)] = instanceEntry{inst: inst}
	}
	return nil
}

// --- persistent-instance health: local TCP probing (r-nacos sniffing) ---
//
// Every node probes on its own; results never ride raft — a flip only
// updates local state and pushes subscribers. Unhealthy instances are
// marked, never removed: persistence means the registration outlives the
// process it points at.

func (s *Service) probeLoop() {
	defer close(s.probeDone)
	t := time.NewTicker(s.probeEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stopProbe:
			return
		case <-t.C:
			s.probePersistent()
		}
	}
}

func (s *Service) probePersistent() {
	// Unique endpoints across services (r-nacos groups by host before
	// probing, so one dead port is dialed once, not once per service).
	s.mu.Lock()
	hosts := map[string]bool{}
	for _, st := range s.services {
		for _, e := range st.instances {
			if !e.inst.Ephemeral {
				hosts[fmt.Sprintf("%s:%d", e.inst.IP, e.inst.Port)] = true
			}
		}
	}
	s.mu.Unlock()
	if len(hosts) == 0 {
		return
	}
	results := make(map[string]bool, len(hosts))
	for addr := range hosts {
		results[addr] = probeTCP(addr, s.probeTimeout)
	}
	s.mu.Lock()
	changed := map[string]Key{}
	for ks, st := range s.services {
		for ik, e := range st.instances {
			if e.inst.Ephemeral {
				continue
			}
			healthy, ok := results[fmt.Sprintf("%s:%d", e.inst.IP, e.inst.Port)]
			if !ok || e.inst.Healthy == healthy {
				continue
			}
			e.inst.Healthy = healthy
			st.instances[ik] = e
			changed[ks] = st.key
		}
	}
	s.mu.Unlock()
	for _, k := range changed {
		go s.push(k)
	}
}

// probeTCP dials once and retries once on failure (r-nacos NetSniffing's
// single retry), guarding against a transient hiccup flipping health.
func probeTCP(addr string, timeout time.Duration) bool {
	for attempt := 0; attempt < 2; attempt++ {
		c, err := net.DialTimeout("tcp", addr, timeout)
		if err == nil {
			_ = c.Close()
			return true
		}
	}
	return false
}
