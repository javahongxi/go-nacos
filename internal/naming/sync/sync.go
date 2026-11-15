// Package sync replicates ephemeral naming state across nodes: connection-
// level AP sync, never through raft (route A, the r-nacos shape). Increments
// broadcast per event through per-peer queues (one retry, r-nacos style); a
// 12s full connID→instances mapping reconciles losses in both directions; an
// independent 3s ping declares peers dead after 15s of silence and purges
// their instances; joiners pull a full snapshot. Subscriptions stay local —
// remote changes just trigger the regular local push path.
package sync

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javahongxi/go-nacos/internal/cluster"
	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/go-nacos/internal/wire"
)

// r-nacos-aligned cadence: 3s ping / 15s death / 12s reconciliation.
const (
	pingEvery      = 3 * time.Second
	deadAfter      = 15 * time.Second
	reconcileEvery = 12 * time.Second
	sendTimeout    = 2 * time.Second
	retryDelay     = 100 * time.Millisecond
	queueSize      = 1024
	pullTimeout    = 5 * time.Second
)

// Manager owns the peer loops and the broadcast queues. It implements
// naming.Emitter (local mutations out) and cluster.Syncer (remote messages
// in); the two directions never re-emit each other, so sync cannot loop.
type Manager struct {
	nodeID string
	svc    *naming.Service
	client *cluster.Client
	peers  func() []string // current voter sync addresses, self included
	logger *slog.Logger

	mu      sync.Mutex
	peerMap map[string]*peerState
	stopped bool

	stopCh   chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
	dropNext atomic.Int64 // e2e loss injection: vanish the next N increments
}

type peerState struct {
	queue    chan outMsg
	stop     chan struct{}
	done     chan struct{}
	lastSeen time.Time
	dead     bool
}

type outMsg struct {
	typeName string
	body     any
}

func New(nodeID string, svc *naming.Service, client *cluster.Client, peers func() []string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		nodeID:  nodeID,
		svc:     svc,
		client:  client,
		peers:   peers,
		logger:  logger,
		peerMap: map[string]*peerState{},
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() {
	m.syncPeers() // workers up front: an increment emitted right after start
	// must not vanish into an empty peer map (reconciliation would heal it,
	// but only after a full 12s cycle)
	m.wg.Add(3)
	go m.pingLoop()
	go m.reconcileLoop()
	go m.pullLoop()
}

func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.wg.Wait() // loops exit first: no new peer workers after this
		m.mu.Lock()
		m.stopped = true
		peers := m.peerMap
		m.peerMap = map[string]*peerState{}
		m.mu.Unlock()
		for _, p := range peers {
			close(p.stop)
			<-p.done
		}
	})
}

// DropNext makes the next n increment broadcasts vanish — e2e loss injection
// proving reconciliation converges without them. Full mappings and pings are
// never dropped.
func (m *Manager) DropNext(n int) { m.dropNext.Add(int64(n)) }

// --- naming.Emitter: local mutations out ---

func (m *Manager) EmitRegister(k naming.Key, inst wire.Instance, connID string) {
	m.broadcast(TypeSyncInstances, instancesMsg{From: m.nodeID, Action: "register", Key: k, ConnID: connID, Inst: inst})
}

func (m *Manager) EmitDeregister(k naming.Key, inst wire.Instance, connID string) {
	m.broadcast(TypeSyncInstances, instancesMsg{From: m.nodeID, Action: "deregister", Key: k, ConnID: connID, Inst: inst})
}

func (m *Manager) EmitRemoveConn(connID string) {
	m.broadcast(TypeRemoveConn, removeConnMsg{From: m.nodeID, ConnID: connID})
}

func (m *Manager) broadcast(typeName string, body any) {
	if typeName != TypeFullMapping && m.dropNext.Add(-1) >= 0 {
		return // injected loss
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	m.syncPeersLocked() // a voter added since the last ping tick must not miss events
	for addr, p := range m.peerMap {
		select {
		case p.queue <- outMsg{typeName: typeName, body: body}:
		default:
			m.logger.Warn("naming sync: peer queue full, dropping (reconciliation will heal)",
				"peer", addr, "type", typeName)
		}
	}
}

// --- peer lifecycle ---

// syncPeers tracks the raft member table: spawn a queue worker per new voter,
// retire workers for removed ones. Naming death does NOT remove raft voters —
// the two membership notions are deliberately decoupled (independent ping).
func (m *Manager) syncPeers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncPeersLocked()
}

func (m *Manager) syncPeersLocked() {
	current := map[string]bool{}
	for _, addr := range m.peers() {
		if addr != m.nodeID {
			current[addr] = true
		}
	}
	for addr := range current {
		if m.peerMap[addr] == nil {
			p := &peerState{
				queue:    make(chan outMsg, queueSize),
				stop:     make(chan struct{}),
				done:     make(chan struct{}),
				lastSeen: time.Now(),
			}
			m.peerMap[addr] = p
			go m.worker(addr, p)
		}
	}
	for addr, p := range m.peerMap {
		if !current[addr] {
			close(p.stop)
			delete(m.peerMap, addr)
		}
	}
}

func (m *Manager) worker(addr string, p *peerState) {
	defer close(p.done)
	for {
		select {
		case <-p.stop:
			return
		case msg := <-p.queue:
			m.send(addr, msg)
		}
	}
}

// send delivers one message with a single retry after 100ms (r-nacos'
// schedule); whatever still slips is healed by the full mapping.
func (m *Manager) send(addr string, msg outMsg) {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	err := m.client.Send(ctx, addr, msg.typeName, msg.body)
	cancel()
	if err == nil {
		return
	}
	select {
	case <-time.After(retryDelay):
	case <-m.stopCh:
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if err := m.client.Send(ctx, addr, msg.typeName, msg.body); err != nil {
		m.logger.Debug("naming sync: send failed", "peer", addr, "type", msg.typeName, "err", err)
	}
}

// touch records peer activity (any inbound message or a ping answer). A dead
// peer coming back triggers a snapshot pull to catch up fast; its own full
// mappings would heal us within 12s anyway.
func (m *Manager) touch(from string) {
	m.mu.Lock()
	p := m.peerMap[from]
	if p != nil {
		p.lastSeen = time.Now()
		if p.dead {
			p.dead = false
			m.logger.Info("naming sync: peer back", "peer", from)
			go m.pullFrom(from)
		}
	}
	m.mu.Unlock()
}

// --- loops ---

func (m *Manager) pingLoop() {
	defer m.wg.Done()
	t := time.NewTicker(pingEvery)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			m.syncPeers()
			now := time.Now()
			m.mu.Lock()
			for addr, p := range m.peerMap {
				if now.Sub(p.lastSeen) > deadAfter {
					if !p.dead {
						p.dead = true
						m.logger.Warn("naming sync: peer dead, purging its instances", "peer", addr)
						go m.svc.PurgeNode(addr)
					}
					continue // resurrection is detected by inbound activity
				}
				go m.ping(addr)
			}
			m.mu.Unlock()
		}
	}
}

func (m *Manager) ping(addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if err := m.client.Send(ctx, addr, TypePing, pingMsg{From: m.nodeID}); err != nil {
		return // silence ages lastSeen; the death check handles it
	}
	m.touch(addr)
}

func (m *Manager) reconcileLoop() {
	defer m.wg.Done()
	t := time.NewTicker(reconcileEvery)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			conns := map[string][]naming.Entry{}
			for _, e := range m.svc.LocalEntries() {
				conns[e.ConnID] = append(conns[e.ConnID], e)
			}
			m.broadcast(TypeFullMapping, fullMappingMsg{From: m.nodeID, Conns: conns})
		}
	}
}

// pullLoop fetches a full snapshot once at startup (new joiner or restarted
// node). Failure is not fatal: full mappings converge within 12s anyway.
func (m *Manager) pullLoop() {
	defer m.wg.Done()
	deadline := time.Now().Add(60 * time.Second)
	wait := time.After(time.Second) // let join/raft settle
	for {
		select {
		case <-m.stopCh:
			return
		case <-wait:
		}
		tried := false
		for _, addr := range m.peers() {
			if addr == m.nodeID {
				continue
			}
			tried = true
			if m.pullFrom(addr) {
				return
			}
		}
		if !tried || time.Now().After(deadline) {
			return // single node, or gave up: reconciliation covers us
		}
		wait = time.After(5 * time.Second)
	}
}

func (m *Manager) pullFrom(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), pullTimeout)
	defer cancel()
	data, err := m.client.PullSnapshot(ctx, addr)
	if err != nil {
		return false
	}
	var snap snapshotMsg
	if err := json.Unmarshal(data, &snap); err != nil {
		return false
	}
	m.svc.RestoreSnapshot(snap.Entries)
	m.logger.Info("naming sync: snapshot restored", "peer", addr, "entries", len(snap.Entries))
	return true
}