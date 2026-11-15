package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/javahongxi/go-nacos/internal/app"
	"github.com/javahongxi/sweet/nacos"
)

// newClusterApp starts one node: join == "" bootstraps a fresh cluster,
// otherwise the node joins through that peer's gRPC address.
func newClusterApp(t *testing.T, dir, join string) *app.App {
	t.Helper()
	return newApp(t, dir, func(o *app.Options) { o.JoinAddr = join })
}

// TestClusterConfigForwarding locks M2a: a follower forwards writes to the
// leader, every node serves reads, and the FSM change hook pushes to local
// watchers on BOTH nodes regardless of where the write landed.
func TestClusterConfigForwarding(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	c1 := newConfig(t, a1.Addr())
	c2 := newConfig(t, a2.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// a2 joined as a follower; a1 stays the leader.
	var rs struct {
		State  string `json:"state"`
		Leader string `json:"leader"`
	}
	adminGet(t, "http://"+a2.AdminAddr()+"/api/raft", &rs)
	if rs.State != "Follower" || rs.Leader != a1.Raft.RaftAddr() {
		t.Fatalf("a2 raft = %+v, want Follower of %s", rs, a1.Raft.RaftAddr())
	}

	key := nacos.ConfigKey{DataID: "cluster.yaml", Group: "DEFAULT_GROUP"}

	// Write through the follower: forwarded to the leader, readable on it.
	if err := c2.Publish(ctx, key, nacos.PublishInput{Content: "via-follower"}); err != nil {
		t.Fatalf("publish via follower: %v", err)
	}
	got, err := c1.Query(ctx, key)
	if err != nil || got.Content != "via-follower" {
		t.Fatalf("leader query after forwarded publish: %+v err=%v", got, err)
	}

	// Watch on the leader node, write via the follower: the FSM hook must
	// notify the leader's local listeners.
	updates1 := make(chan nacos.Config, 8)
	if err := c1.Watch(ctx, key, func(c nacos.Config) { updates1 <- c }); err != nil {
		t.Fatalf("watch on leader node: %v", err)
	}
	select {
	case seed := <-updates1:
		if seed.Content != "via-follower" {
			t.Fatalf("watch seed = %q", seed.Content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch seed never arrived")
	}
	if err := c2.Publish(ctx, key, nacos.PublishInput{Content: "v2"}); err != nil {
		t.Fatalf("publish v2 via follower: %v", err)
	}
	waitContent(t, updates1, "v2")

	// And the reverse: watch on the follower node, write on the leader.
	updates2 := make(chan nacos.Config, 8)
	if err := c2.Watch(ctx, key, func(c nacos.Config) { updates2 <- c }); err != nil {
		t.Fatalf("watch on follower node: %v", err)
	}
	// Drain the seed: the follower FSM trails the leader by a heartbeat, so
	// the seed may still be the previous content.
	select {
	case <-updates2:
	case <-time.After(5 * time.Second):
		t.Fatal("follower watch seed never arrived")
	}
	if err := c1.Publish(ctx, key, nacos.PublishInput{Content: "v3"}); err != nil {
		t.Fatalf("publish v3 on leader: %v", err)
	}
	waitContent(t, updates2, "v3")

	// Reads converge on the follower too (raft apply is async there).
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err = c2.Query(ctx, key)
		if err == nil && got.Content == "v3" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower query = %+v err=%v, want v3", got, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Remove via the follower reaches both nodes.
	if err := c2.Remove(ctx, key); err != nil {
		t.Fatalf("remove via follower: %v", err)
	}
	if _, err := c1.Query(ctx, key); err == nil {
		t.Fatal("leader still serves the removed key")
	}
}

// waitContent drains config pushes until the wanted content shows up —
// follower FSMs trail the leader, so intermediate stale pushes are legal.
func waitContent(t *testing.T, updates <-chan nacos.Config, want string) {
	t.Helper()
	deadline := time.After(8 * time.Second)
	for {
		select {
		case c := <-updates:
			if c.Content == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for config content %q", want)
		}
	}
}

// waitQueryHosts polls Query until the service shows the wanted host count —
// naming sync is AP and asynchronous, so reads converge rather than snap.
func waitQueryHosts(t *testing.T, ctx context.Context, c *nacos.NamingClient, svc string, want int) {
	t.Helper()
	// 15s: worst case is a lost increment healed by the 12s full mapping.
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := c.Query(ctx, svc)
		if err == nil && len(info.Hosts) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for query %s to have %d hosts (last=%+v err=%v)", svc, want, info, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitHostsWithin is waitHosts with an explicit budget — the death test must
// outlast the 15s ping timeout.
func waitHostsWithin(t *testing.T, updates <-chan nacos.ServiceInfo, want int, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case info := <-updates:
			if len(info.Hosts) == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a push with %d hosts", want)
		}
	}
}

// TestClusterNamingSync locks M2b mutual visibility: registrations on either
// node are queryable AND push to subscribers on the other; deregister and
// connection death propagate the same way.
func TestClusterNamingSync(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	provider1 := newNaming(t, a1.Addr())
	provider2 := newNaming(t, a2.Addr())
	consumer1 := newNaming(t, a1.Addr())
	consumer2 := newNaming(t, a2.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const svc = "cluster.Sync"
	inst1 := nacos.Instance{IP: "127.0.0.1", Port: 24000, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	inst2 := nacos.Instance{IP: "127.0.0.1", Port: 24001, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}

	// Register on a1: a2 must see it through the increment broadcast.
	if err := provider1.Register(ctx, svc, inst1); err != nil {
		t.Fatalf("register inst1 on a1: %v", err)
	}
	waitQueryHosts(t, ctx, consumer2, svc, 1)
	if _, err := consumer1.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe on a1: %v", err)
	}
	if _, err := consumer2.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe on a2: %v", err)
	}

	// Register on a2: both subscribers get the cross-node push.
	if err := provider2.Register(ctx, svc, inst2); err != nil {
		t.Fatalf("register inst2 on a2: %v", err)
	}
	waitHosts(t, consumer1.Updates(), 2)
	waitHosts(t, consumer2.Updates(), 2)

	// Deregister propagates both ways.
	if err := provider2.Deregister(ctx, svc, inst2); err != nil {
		t.Fatalf("deregister inst2: %v", err)
	}
	waitHosts(t, consumer1.Updates(), 1)
	waitHosts(t, consumer2.Updates(), 1)

	// Connection death on a1 removes its instance everywhere.
	provider1.Close()
	waitHosts(t, consumer1.Updates(), 0)
	waitHosts(t, consumer2.Updates(), 0)
}

// TestClusterNodeDeath locks the independent-ping death path: a crashed node
// sends no goodbye, so its instances must be purged within the 15s silence
// window — even though raft has lost quorum (naming stays AP).
func TestClusterNodeDeath(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	provider := newNaming(t, a1.Addr())
	consumer := newNaming(t, a2.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const svc = "cluster.Death"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 24010, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register on a1: %v", err)
	}
	waitQueryHosts(t, ctx, consumer, svc, 1)
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe on a2: %v", err)
	}

	// Crash a1: no connection retirement, no sync goodbye.
	a1.HardStop()
	waitHostsWithin(t, consumer.Updates(), 0, 25*time.Second)
	waitQueryHosts(t, ctx, consumer, svc, 0)
}

// TestClusterNewNodeSnapshot locks the joiner path: a third node pulls the
// full state from a live peer and then participates in both directions.
func TestClusterNewNodeSnapshot(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	provider1 := newNaming(t, a1.Addr())
	provider2 := newNaming(t, a2.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const svc = "cluster.Snapshot"
	inst1 := nacos.Instance{IP: "127.0.0.1", Port: 24020, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	inst2 := nacos.Instance{IP: "127.0.0.1", Port: 24021, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider1.Register(ctx, svc, inst1); err != nil {
		t.Fatalf("register inst1: %v", err)
	}
	if err := provider2.Register(ctx, svc, inst2); err != nil {
		t.Fatalf("register inst2: %v", err)
	}
	probe := newNaming(t, a1.Addr())
	waitQueryHosts(t, ctx, probe, svc, 2)

	// a3 joins with an empty naming state: the snapshot pull must fill it.
	a3 := newClusterApp(t, t.TempDir(), a1.Addr())
	consumer3 := newNaming(t, a3.Addr())
	waitQueryHosts(t, ctx, consumer3, svc, 2)
	if _, err := consumer3.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe on a3: %v", err)
	}

	// And a3's own registrations flow back to the old nodes.
	provider3 := newNaming(t, a3.Addr())
	inst3 := nacos.Instance{IP: "127.0.0.1", Port: 24022, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider3.Register(ctx, svc, inst3); err != nil {
		t.Fatalf("register inst3 on a3: %v", err)
	}
	waitHosts(t, consumer3.Updates(), 3)
	waitQueryHosts(t, ctx, probe, svc, 3)
}

// TestClusterReconcile locks loss healing: dropped increments must leave the
// peer stale until the 12s full mapping converges it — for both register and
// deregister.
func TestClusterReconcile(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	provider := newNaming(t, a1.Addr())
	consumer := newNaming(t, a2.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const svc = "cluster.Reconcile"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 24030, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe on a2: %v", err)
	}

	// Drop the register increment: a2 stays empty for now (short window,
	// well inside the 12s reconciliation period)...
	a1.Sync.DropNext(1)
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register with dropped increment: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if info, err := consumer.Query(ctx, svc); err == nil && len(info.Hosts) != 0 {
		t.Fatalf("dropped register leaked through: %+v", info)
	}
	// ...then the full mapping heals it.
	waitHostsWithin(t, consumer.Updates(), 1, 15*time.Second)

	// Same for deregister: the stale replica survives until reconciliation.
	a1.Sync.DropNext(1)
	if err := provider.Deregister(ctx, svc, inst); err != nil {
		t.Fatalf("deregister with dropped increment: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if info, err := consumer.Query(ctx, svc); err != nil || len(info.Hosts) != 1 {
		t.Fatalf("dropped deregister leaked through: %+v err=%v", info, err)
	}
	waitHostsWithin(t, consumer.Updates(), 0, 15*time.Second)
}