package e2e

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
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
	if rs.State != "Follower" || rs.Leader != a1.Addr() {
		t.Fatalf("a2 raft = %+v, want Follower of %s", rs, a1.Addr())
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

// raftMembers reads a node's current member table from its console.
func raftMembers(t *testing.T, a *app.App) []string {
	t.Helper()
	var rs struct {
		Members []struct {
			ID string `json:"id"`
		} `json:"members"`
	}
	adminGet(t, "http://"+a.AdminAddr()+"/api/raft", &rs)
	ids := make([]string, 0, len(rs.Members))
	for _, m := range rs.Members {
		ids = append(ids, m.ID)
	}
	return ids
}

// waitMembers polls until the member table equals want (order-independent).
func waitMembers(t *testing.T, a *app.App, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := raftMembers(t, a)
		if len(got) == len(want) {
			match := true
			for _, w := range want {
				if !slices.Contains(got, w) {
					match = false
				}
			}
			if match {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("member table on %s = %v, want %v", a.Addr(), got, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitDone waits for a removed node to self-stop (the membership watcher).
func waitDone(t *testing.T, a *app.App, d time.Duration) {
	t.Helper()
	select {
	case <-a.Done():
	case <-time.After(d):
		t.Fatalf("node %s did not self-stop after removal", a.Addr())
	}
}

// TestClusterRemoveFollower locks the scale-down path: removing a follower
// shrinks every member table, sweeps its naming replicas on the survivors,
// self-stops the victim, and leaves config writes working at quorum 2/2.
func TestClusterRemoveFollower(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	a3 := newClusterApp(t, t.TempDir(), a1.Addr())
	victim := a3.Addr()
	provider := newNaming(t, a3.Addr())
	consumer := newNaming(t, a2.Addr())
	cfg1 := newConfig(t, a1.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const svc = "cluster.Remove"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 24040, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register on a3: %v", err)
	}
	waitQueryHosts(t, ctx, consumer, svc, 1)
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe on a2: %v", err)
	}

	// Remove a3 through the leader's console.
	adminWrite(t, http.MethodDelete, "http://"+a1.AdminAddr()+"/api/raft/member?node="+victim, nil, http.StatusOK)

	// a3 self-stops; survivors shrink their tables and sweep its replicas.
	waitDone(t, a3, 10*time.Second)
	waitHostsWithin(t, consumer.Updates(), 0, 10*time.Second)
	waitQueryHosts(t, ctx, consumer, svc, 0)
	waitMembers(t, a1, a1.Addr(), a2.Addr())
	waitMembers(t, a2, a1.Addr(), a2.Addr())

	// Config still writes with quorum 2/2.
	key := nacos.ConfigKey{DataID: "after-remove.yaml", Group: "DEFAULT_GROUP"}
	if err := cfg1.Publish(ctx, key, nacos.PublishInput{Content: "ok"}); err != nil {
		t.Fatalf("publish after removal: %v", err)
	}
	got, err := cfg1.Query(ctx, key)
	if err != nil || got.Content != "ok" {
		t.Fatalf("query after removal: %+v err=%v", got, err)
	}
}

// TestClusterRemoveRedirect locks leader routing: a remove issued on a
// follower's console is redirected to the leader and takes effect.
func TestClusterRemoveRedirect(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	a3 := newClusterApp(t, t.TempDir(), a1.Addr())
	victim := a3.Addr()
	adminWrite(t, http.MethodDelete, "http://"+a2.AdminAddr()+"/api/raft/member?node="+victim, nil, http.StatusOK)
	waitDone(t, a3, 10*time.Second)
	waitMembers(t, a1, a1.Addr(), a2.Addr())
}

// TestClusterRemoveLeader locks leader self-removal: the remaining two elect
// a new leader, writes recover, replicated data survives, and the old leader
// self-stops.
func TestClusterRemoveLeader(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	a3 := newClusterApp(t, t.TempDir(), a1.Addr())
	leader := a1.Addr()
	cfg2 := newConfig(t, a2.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "leader-remove.yaml", Group: "DEFAULT_GROUP"}
	if err := cfg2.Publish(ctx, key, nacos.PublishInput{Content: "before"}); err != nil {
		t.Fatalf("publish before removal: %v", err)
	}

	// Remove the leader through its own console.
	adminWrite(t, http.MethodDelete, "http://"+a1.AdminAddr()+"/api/raft/member?node="+leader, nil, http.StatusOK)
	waitDone(t, a1, 10*time.Second)

	// The remaining two elect a new leader; writes recover and data survives.
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := cfg2.Publish(ctx, key, nacos.PublishInput{Content: "after"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("write never recovered after leader removal: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// a2 may still be a follower: its FSM trails the leader, so poll the read
	// until it converges (the same async-apply semantics as forwarding).
	qdeadline := time.Now().Add(5 * time.Second)
	for {
		got, err := cfg2.Query(ctx, key)
		if err == nil && got.Content == "after" {
			break
		}
		if time.Now().After(qdeadline) {
			t.Fatalf("query after leader removal: %+v err=%v", got, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitMembers(t, a2, a2.Addr(), a3.Addr())
	waitMembers(t, a3, a2.Addr(), a3.Addr())
}

// TestClusterRemoveGuards locks the refusal rails: the last voter cannot be
// removed, an unknown node is idempotent OK, and a missing param is a 400.
func TestClusterRemoveGuards(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	base := "http://" + a1.AdminAddr()
	adminWrite(t, http.MethodDelete, base+"/api/raft/member?node="+a1.Addr(), nil, http.StatusConflict)
	adminWrite(t, http.MethodDelete, base+"/api/raft/member?node=127.0.0.1:1", nil, http.StatusOK)
	adminWrite(t, http.MethodDelete, base+"/api/raft/member", nil, http.StatusBadRequest)
	waitMembers(t, a1, a1.Addr())
}

// TestClusterRemovedRejoin locks the re-add path: a removed node restarted
// with the same identity and data dir re-joins idempotently and refills its
// naming state from a peer snapshot.
func TestClusterRemovedRejoin(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	provider1 := newNaming(t, a1.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const svc = "cluster.Rejoin"
	inst1 := nacos.Instance{IP: "127.0.0.1", Port: 24050, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider1.Register(ctx, svc, inst1); err != nil {
		t.Fatalf("register on a1: %v", err)
	}

	// a2 joins on a pinned identity so a restart keeps it.
	grpcAddr := pinnedGRPCListen(t)
	dir := t.TempDir()
	joinPin := func(o *app.Options) { o.GRPCListen = grpcAddr; o.JoinAddr = a1.Addr() }
	a2 := newApp(t, dir, joinPin)
	consumer2 := newNaming(t, a2.Addr())
	waitQueryHosts(t, ctx, consumer2, svc, 1) // snapshot pull filled it

	// Remove a2; it self-stops.
	adminWrite(t, http.MethodDelete, "http://"+a1.AdminAddr()+"/api/raft/member?node="+grpcAddr, nil, http.StatusOK)
	waitDone(t, a2, 10*time.Second)
	waitMembers(t, a1, a1.Addr())

	// Restart with the same identity and data dir, re-joining explicitly.
	a2b := newApp(t, dir, joinPin)
	consumer2b := newNaming(t, a2b.Addr())
	waitQueryHosts(t, ctx, consumer2b, svc, 1)
	waitMembers(t, a1, a1.Addr(), grpcAddr)
}

// raftMemberTable reads id → suffrage from a node's console.
func raftMemberTable(t *testing.T, a *app.App) map[string]string {
	t.Helper()
	var rs struct {
		Members []struct {
			ID       string `json:"id"`
			Suffrage string `json:"suffrage"`
		} `json:"members"`
	}
	adminGet(t, "http://"+a.AdminAddr()+"/api/raft", &rs)
	m := make(map[string]string, len(rs.Members))
	for _, x := range rs.Members {
		m[x.ID] = x.Suffrage
	}
	return m
}

// waitSuffrage polls until the member holds the wanted suffrage.
func waitSuffrage(t *testing.T, a *app.App, id, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := raftMemberTable(t, a)[id]; got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("member %s on %s never reached suffrage %s (table=%v)", id, a.Addr(), want, raftMemberTable(t, a))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestClusterJoinLearnerPhase locks the learner gate: while the joiner
// catches up it is a Nonvoter — the voter set (and thus the quorum) stays
// unchanged and the leader keeps committing alone — and promotion happens
// only after the joiner holds the log.
func TestClusterJoinLearnerPhase(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	c1 := newConfig(t, a1.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "learner.yaml", Group: "DEFAULT_GROUP"}
	if err := c1.Publish(ctx, key, nacos.PublishInput{Content: "before-join"}); err != nil {
		t.Fatalf("publish before join: %v", err)
	}
	// Some log bulk so catch-up is real work, not a no-op.
	bulk := nacos.ConfigKey{DataID: "learner-bulk.yaml", Group: "DEFAULT_GROUP"}
	if err := c1.Publish(ctx, bulk, nacos.PublishInput{Content: strings.Repeat("x", 256<<10)}); err != nil {
		t.Fatalf("publish bulk: %v", err)
	}

	// A wide probe cadence guarantees an observable learner window: the
	// gate sleeps one poll before its first probe.
	a2, err := app.New(app.Options{
		GRPCListen: "127.0.0.1:0", RaftListen: "127.0.0.1:0", AdminListen: "127.0.0.1:0",
		DataDir: t.TempDir(), JoinAddr: a1.Addr(),
		JoinCatchUpPoll: 500 * time.Millisecond,
		Logger:          discardLogger(),
	})
	if err != nil {
		t.Fatalf("app.New a2: %v", err)
	}
	t.Cleanup(a2.Stop)
	started := make(chan error, 1)
	go func() { started <- a2.Start() }()

	// Catch the learner window on the leader's member table.
	var table map[string]string
	deadline := time.Now().Add(10 * time.Second)
	for {
		table = raftMemberTable(t, a1)
		if table[a2.Addr()] == "Nonvoter" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never observed the learner phase (table=%v)", table)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Quorum unchanged: the joiner is present but only a1 votes.
	numVoters := 0
	for _, s := range table {
		if s == "Voter" {
			numVoters++
		}
	}
	if numVoters != 1 {
		t.Fatalf("voter count during learner phase = %d, want 1 (table=%v)", numVoters, table)
	}
	// The lone voter still commits while the learner catches up.
	during := nacos.ConfigKey{DataID: "during-learner.yaml", Group: "DEFAULT_GROUP"}
	if err := c1.Publish(ctx, during, nacos.PublishInput{Content: "quorum-1"}); err != nil {
		t.Fatalf("publish during learner phase: %v", err)
	}

	if err := <-started; err != nil {
		t.Fatalf("a2.Start: %v", err)
	}
	// Promotion after catch-up: a2 votes and serves the pre-join log.
	waitSuffrage(t, a1, a2.Addr(), "Voter")
	waitMembers(t, a1, a1.Addr(), a2.Addr())
	c2 := newConfig(t, a2.Addr())
	for _, k := range []nacos.ConfigKey{key, bulk, during} {
		want := "before-join"
		switch k {
		case bulk:
			want = strings.Repeat("x", 256<<10)
		case during:
			want = "quorum-1"
		}
		qdeadline := time.Now().Add(5 * time.Second)
		for {
			got, err := c2.Query(ctx, k)
			if err == nil && got.Content == want {
				break
			}
			if time.Now().After(qdeadline) {
				t.Fatalf("a2 query %s: %+v err=%v, want caught-up content", k.DataID, got, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TestClusterJoinLearnerResume locks the interrupted-join path: a joiner
// that dies mid-catch-up stays a Nonvoter (never promoted, quorum intact),
// and restarting it with the same identity resumes the gate to promotion.
func TestClusterJoinLearnerResume(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	c1 := newConfig(t, a1.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "resume.yaml", Group: "DEFAULT_GROUP"}
	if err := c1.Publish(ctx, key, nacos.PublishInput{Content: "durable"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Pin both ports: the restart must keep the gRPC identity AND the raft
	// transport address registered in the leader's configuration.
	grpcAddr := pinnedGRPCListen(t)
	raftAddr := pinnedGRPCListen(t)
	dir := t.TempDir()
	opts := app.Options{
		GRPCListen: grpcAddr, RaftListen: raftAddr, AdminListen: "127.0.0.1:0",
		DataDir: dir, JoinAddr: a1.Addr(),
		JoinCatchUpPoll: 2 * time.Second,
		Logger:          discardLogger(),
	}
	a2, err := app.New(opts)
	if err != nil {
		t.Fatalf("app.New a2: %v", err)
	}
	t.Cleanup(a2.Stop)
	go func() { _ = a2.Start() }()

	waitSuffrage(t, a1, grpcAddr, "Nonvoter")
	// Crash the joiner mid-catch-up: the gate's RPC context dies with it,
	// so the leader must NOT promote a dead node.
	a2.HardStop()
	time.Sleep(500 * time.Millisecond)
	if got := raftMemberTable(t, a1)[grpcAddr]; got != "Nonvoter" {
		t.Fatalf("dead joiner suffrage = %q, want stranded Nonvoter", got)
	}
	// Quorum intact: the lone voter keeps writing.
	if err := c1.Publish(ctx, key, nacos.PublishInput{Content: "after-crash"}); err != nil {
		t.Fatalf("publish while joiner stranded: %v", err)
	}

	// Same identity, same data dir: the re-join resumes at the gate and
	// promotes once caught up.
	a2b, err := app.New(opts)
	if err != nil {
		t.Fatalf("app.New a2b: %v", err)
	}
	t.Cleanup(a2b.Stop)
	if err := a2b.Start(); err != nil {
		t.Fatalf("a2b.Start: %v", err)
	}
	waitSuffrage(t, a1, grpcAddr, "Voter")
	waitMembers(t, a1, a1.Addr(), grpcAddr)
	c2 := newConfig(t, a2b.Addr())
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := c2.Query(ctx, key)
		if err == nil && got.Content == "after-crash" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a2b query after resume: %+v err=%v", got, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// discardLogger silences app logs in tests that build app.Options by hand.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}