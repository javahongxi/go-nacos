// Package conn tracks client connections. Identity is the peer address — the
// same TCP connection carries both the unary calls and the bidi stream, so
// unary dispatch and stream pushes meet at one Connection, mirroring Nacos'
// transport-derived connectionId (AddressTransportFilter).
package conn

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javahongxi/sweet/gen"
)

// Push semantics copied from RpcConfigChangeNotifier: 3s ack timeout, retries
// at 0/2/4s, unregister the connection after 3 unacked attempts.
const (
	pushAckTimeout = 3 * time.Second
	pushMaxTries   = 3
)

// Meta is what the ConnectionSetupRequest told us.
type Meta struct {
	ClientIP string
	Version  string
	Module   string
	AppName  string
	Labels   map[string]string
}

// Connection is one registered client. send is owned by the bidi stream
// handler (serialised there); pending maps push requestId → ack channel.
type Connection struct {
	ID   string
	Peer string
	Meta Meta

	send func(*gen.Payload) error
	done chan struct{}

	mu      sync.Mutex
	closed  bool
	pending map[string]chan struct{}
	seq     atomic.Int64

	closeOnce sync.Once
}

// Done is closed when the connection retires (stream end, replacement, or
// push-ack exhaustion). Stream handlers select on it to exit.
func (c *Connection) Done() <-chan struct{} { return c.done }

// NextRequestID mints push correlation ids, unique within this connection.
func (c *Connection) NextRequestID() string {
	return fmt.Sprintf("s%d", c.seq.Add(1))
}

// Ack wakes the push waiting on requestId. Called by the stream handler for
// every Response frame carrying a requestId.
func (c *Connection) Ack(requestID string) {
	c.mu.Lock()
	ch := c.pending[requestID]
	delete(c.pending, requestID)
	c.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// Registry maps peer addr → current connection, and id → connection for
// push fan-out. hooks run on every retirement (domain cleanup).
type Registry struct {
	mu     sync.RWMutex
	byPeer map[string]*Connection
	byID   map[string]*Connection
	hooks  []func(connID string)
	logger *slog.Logger
	nodeID string
	idSeq  atomic.Int64
}

func NewRegistry(nodeID string, logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry{
		byPeer: map[string]*Connection{},
		byID:   map[string]*Connection{},
		logger: logger,
		nodeID: nodeID,
	}
}

// OnClose registers a cleanup hook (config listens, naming instances and
// subscriptions). Hooks run synchronously on retirement.
func (r *Registry) OnClose(hook func(connID string)) {
	r.mu.Lock()
	r.hooks = append(r.hooks, hook)
	r.mu.Unlock()
}

// Register installs the connection for peer, retiring any previous one — a
// reconnect on a new source port must not leave the old identity alive.
func (r *Registry) Register(peer string, meta Meta, send func(*gen.Payload) error) *Connection {
	c := &Connection{
		// nodeID_millis_seq_peer: the node prefix makes ids cluster-unique
		// (sync guards compare them across nodes); the seq guards against
		// same-millisecond reconnects.
		ID:      fmt.Sprintf("%s_%d_%d_%s", r.nodeID, time.Now().UnixMilli(), r.idSeq.Add(1), peer),
		Peer:    peer,
		Meta:    meta,
		send:    send,
		done:    make(chan struct{}),
		pending: map[string]chan struct{}{},
	}
	r.mu.Lock()
	old := r.byPeer[peer]
	r.byPeer[peer] = c
	r.byID[c.ID] = c
	hooks := r.hooks
	r.mu.Unlock()
	if old != nil {
		r.retire(old, hooks)
	}
	return c
}

// Unregister retires the connection only if it is still the current one for
// peer: a dying old stream must not clean up the connection that replaced it.
func (r *Registry) Unregister(peer, id string) {
	r.mu.Lock()
	c := r.byPeer[peer]
	if c == nil || c.ID != id {
		r.mu.Unlock()
		return
	}
	delete(r.byPeer, peer)
	hooks := r.hooks
	r.mu.Unlock()
	r.retire(c, hooks)
}

func (r *Registry) Get(peer string) *Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byPeer[peer]
}

func (r *Registry) GetByID(id string) *Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[id]
}

// ConnInfo is a read-only snapshot of a connection for the admin console.
type ConnInfo struct {
	ID       string `json:"id"`
	Peer     string `json:"peer"`
	ClientIP string `json:"clientIp"`
	Version  string `json:"version"`
	Module   string `json:"module"`
	AppName  string `json:"appName"`
}

// List returns every live connection, ordered by peer then id. Read-only.
func (r *Registry) List() []ConnInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ConnInfo, 0, len(r.byID))
	for _, c := range r.byID {
		out = append(out, ConnInfo{
			ID: c.ID, Peer: c.Peer, ClientIP: c.Meta.ClientIP,
			Version: c.Meta.Version, Module: c.Meta.Module, AppName: c.Meta.AppName,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Peer != out[j].Peer {
			return out[i].Peer < out[j].Peer
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// CloseAll retires every connection (server shutdown, or the e2e reconnect
// hook). Stream handlers see done close and exit, so GracefulStop can finish.
func (r *Registry) CloseAll() {
	r.mu.Lock()
	conns := make([]*Connection, 0, len(r.byID))
	for _, c := range r.byID {
		conns = append(conns, c)
	}
	r.byPeer = map[string]*Connection{}
	hooks := r.hooks
	r.mu.Unlock()
	for _, c := range conns {
		r.retire(c, hooks)
	}
}

// retire is idempotent: close done, drop from byID, wake pending pushes, run
// cleanup hooks exactly once.
func (r *Registry) retire(c *Connection, hooks []func(string)) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		pending := c.pending
		c.pending = map[string]chan struct{}{}
		c.mu.Unlock()
		for _, ch := range pending {
			close(ch)
		}
		close(c.done)
		r.mu.Lock()
		if r.byID[c.ID] == c {
			delete(r.byID, c.ID)
		}
		r.mu.Unlock()
		for _, h := range hooks {
			h(c.ID)
		}
		r.logger.Debug("connection retired", "conn", c.ID, "module", c.Meta.Module)
	})
}

// Push sends a server request and waits for the client's Response ack, with
// Nacos' retry schedule. Exhausting the retries retires the connection — the
// client is considered dead, exactly like RpcConfigChangeNotifier.unregister.
func (r *Registry) Push(c *Connection, p *gen.Payload, requestID string) error {
	for try := 0; try < pushMaxTries; try++ {
		if try > 0 {
			select {
			case <-c.done:
				return fmt.Errorf("conn: %s retired", c.ID)
			case <-time.After(time.Duration(try) * 2 * time.Second):
			}
		}
		ack := make(chan struct{})
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return fmt.Errorf("conn: %s retired", c.ID)
		}
		c.pending[requestID] = ack
		c.mu.Unlock()

		if err := c.send(p); err != nil {
			c.mu.Lock()
			delete(c.pending, requestID)
			c.mu.Unlock()
			return fmt.Errorf("conn: send to %s: %w", c.ID, err)
		}
		select {
		case <-ack:
			// retire also closes pending channels; a wake is only a real ack
			// if the connection is still open.
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()
			if !closed {
				return nil
			}
		case <-time.After(pushAckTimeout):
			c.mu.Lock()
			delete(c.pending, requestID)
			c.mu.Unlock()
		case <-c.done:
			return fmt.Errorf("conn: %s retired", c.ID)
		}
	}
	r.logger.Warn("push unacked, unregistering connection", "conn", c.ID, "requestId", requestID)
	r.mu.Lock()
	if r.byPeer[c.Peer] == c {
		delete(r.byPeer, c.Peer)
	}
	hooks := r.hooks
	r.mu.Unlock()
	r.retire(c, hooks)
	return fmt.Errorf("conn: push to %s unacked after %d tries", c.ID, pushMaxTries)
}
