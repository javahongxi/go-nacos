package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/javahongxi/go-nacos/internal/app"
	"github.com/javahongxi/sweet/nacos"
)

// livePort opens a real TCP listener so persistent-instance probes see a
// healthy endpoint; closing it flips health without any mock.
func livePort(t *testing.T) (net.Listener, int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, l.Addr().(*net.TCPAddr).Port
}

type historyResp struct {
	Total   int `json:"total"`
	History []struct {
		Content string `json:"content"`
	} `json:"history"`
}

func getHistory(t *testing.T, adminAddr, dataID string) historyResp {
	t.Helper()
	var out historyResp
	adminGet(t, "http://"+adminAddr+"/api/config/history?dataId="+dataID, &out)
	return out
}

// TestConfigHistory locks the r-nacos histories semantics: versions live in
// the FSM (newest first from the admin API), an unchanged republish adds no
// version, the list caps at 100, restart keeps it (raft replay), and removing
// the config drops its history.
func TestConfigHistory(t *testing.T) {
	dir := t.TempDir()
	grpcAddr := pinnedGRPCListen(t)
	pin := func(o *app.Options) { o.GRPCListen = grpcAddr }
	a := newApp(t, dir, pin)
	c := newConfig(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "history.yaml", Group: "DEFAULT_GROUP"}
	for _, v := range []string{"v1", "v2", "v3"} {
		if err := c.Publish(ctx, key, nacos.PublishInput{Content: v}); err != nil {
			t.Fatalf("publish %s: %v", v, err)
		}
	}
	h := getHistory(t, a.AdminAddr(), "history.yaml")
	if h.Total != 3 || h.History[0].Content != "v3" || h.History[2].Content != "v1" {
		t.Fatalf("history = %+v, want 3 versions newest-first v3..v1", h)
	}

	// Same content again: no new version (r-nacos early-returns on equal md5).
	if err := c.Publish(ctx, key, nacos.PublishInput{Content: "v3"}); err != nil {
		t.Fatalf("republish v3: %v", err)
	}
	if h := getHistory(t, a.AdminAddr(), "history.yaml"); h.Total != 3 {
		t.Fatalf("unchanged republish grew history to %d", h.Total)
	}

	// Overflow the 100-version cap.
	for i := 0; i <= 100; i++ {
		if err := c.Publish(ctx, key, nacos.PublishInput{Content: fmt.Sprintf("bulk-%d", i)}); err != nil {
			t.Fatalf("publish bulk-%d: %v", i, err)
		}
	}
	h = getHistory(t, a.AdminAddr(), "history.yaml")
	if h.Total != 100 || h.History[0].Content != "bulk-100" || h.History[99].Content != "bulk-1" {
		t.Fatalf("capped history = total %d first %q last %q, want 100 / bulk-100 / bulk-1",
			h.Total, h.History[0].Content, h.History[99].Content)
	}

	// Restart: history survives through raft log replay alone (no side db).
	c.Close()
	a.Stop()
	a2 := newApp(t, dir, pin)
	h = getHistory(t, a2.AdminAddr(), "history.yaml")
	if h.Total != 100 || h.History[0].Content != "bulk-100" {
		t.Fatalf("history after restart = total %d first %q, want 100 / bulk-100", h.Total, h.History[0].Content)
	}

	// Remove drops the history with the config.
	c2 := newConfig(t, a2.Addr())
	if err := c2.Remove(ctx, key); err != nil {
		t.Fatalf("remove: %v", err)
	}
	resp, err := http.Get("http://" + a2.AdminAddr() + "/api/config/history?dataId=history.yaml")
	if err != nil {
		t.Fatalf("history after remove: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("history after remove status = %d, want 404", resp.StatusCode)
	}
}

// TestNamingPersistent locks the CP lifecycle: a persistent instance has no
// owning connection (survives disconnect), survives a node restart through
// raft replay, rejects ephemeral takeover of its endpoint, and deregisters
// through raft. An ephemeral sibling on the same service dies with its conn.
func TestNamingPersistent(t *testing.T) {
	dir := t.TempDir()
	grpcAddr := pinnedGRPCListen(t)
	pin := func(o *app.Options) { o.GRPCListen = grpcAddr }
	a := newApp(t, dir, pin)
	provider := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const svc = "persist.Svc"
	_, port := livePort(t)
	persistent := nacos.Instance{IP: "127.0.0.1", Port: port, Weight: 1, Healthy: true, Enabled: true, Ephemeral: false}
	ephemeral := nacos.Instance{IP: "127.0.0.1", Port: port + 1, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, persistent); err != nil {
		t.Fatalf("register persistent: %v", err)
	}
	if err := provider.Register(ctx, svc, ephemeral); err != nil {
		t.Fatalf("register ephemeral: %v", err)
	}
	waitQueryHosts(t, ctx, provider, svc, 2)

	// An ephemeral registration must not steal the persistent endpoint.
	steal := persistent
	steal.Ephemeral = true
	if err := provider.Register(ctx, svc, steal); err == nil {
		t.Fatal("ephemeral takeover of a persistent endpoint succeeded")
	}

	// Disconnect: the ephemeral sibling goes, the persistent one stays.
	provider.Close()
	consumer := newNaming(t, a.Addr())
	waitQueryHosts(t, ctx, consumer, svc, 1)
	info, err := consumer.Query(ctx, svc)
	if err != nil || len(info.Hosts) != 1 || info.Hosts[0].Ephemeral || info.Hosts[0].Port != port {
		t.Fatalf("after disconnect hosts = %+v err=%v, want only the persistent endpoint", info.Hosts, err)
	}

	// Restart: raft replay brings the persistent instance back.
	consumer.Close()
	a.Stop()
	a2 := newApp(t, dir, pin)
	consumer2 := newNaming(t, a2.Addr())
	info, err = consumer2.Query(ctx, svc)
	if err != nil || len(info.Hosts) != 1 || info.Hosts[0].Ephemeral || info.Hosts[0].Port != port {
		t.Fatalf("after restart hosts = %+v err=%v, want the persistent endpoint", info.Hosts, err)
	}

	// Deregister through raft removes it.
	if err := consumer2.Deregister(ctx, svc, persistent); err != nil {
		t.Fatalf("deregister persistent: %v", err)
	}
	waitQueryHosts(t, ctx, consumer2, svc, 0)
}

// TestNamingPersistentHealth locks the r-nacos sniffing shape: local TCP
// probes flip health both ways, push subscribers, and never remove the
// registration — persistence means outliving the process it points at.
func TestNamingPersistentHealth(t *testing.T) {
	a := newApp(t, t.TempDir(), func(o *app.Options) {
		o.NamingProbeInterval = 300 * time.Millisecond
		o.NamingProbeTimeout = 200 * time.Millisecond
	})
	provider := newNaming(t, a.Addr())
	consumer := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const svc = "persist.Health"
	l, port := livePort(t)
	inst := nacos.Instance{IP: "127.0.0.1", Port: port, Weight: 1, Healthy: true, Enabled: true, Ephemeral: false}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register persistent: %v", err)
	}
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	waitHealthy := func(want bool) {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for {
			info, err := consumer.Query(ctx, svc)
			if err == nil && len(info.Hosts) == 1 && info.Hosts[0].Healthy == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for healthy=%v (last=%+v err=%v)", want, info, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitHealthy(true)

	// Kill the endpoint: unhealthy, but still registered.
	l.Close()
	waitHealthy(false)

	// Bring it back on the same port: healthy again.
	l2, err := net.Listen("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("relisten: %v", err)
	}
	defer l2.Close()
	waitHealthy(true)

	// The subscriber saw the flips as pushes (drain until both observed).
	sawDown, sawUp := false, false
	deadline := time.After(8 * time.Second)
	for !sawDown || !sawUp {
		select {
		case info := <-consumer.Updates():
			if len(info.Hosts) == 1 {
				if !info.Hosts[0].Healthy {
					sawDown = true
				} else if sawDown {
					sawUp = true
				}
			}
		case <-deadline:
			t.Fatalf("subscriber pushes missing flips (down=%v up=%v)", sawDown, sawUp)
		}
	}
}

// TestClusterPersistentInstance locks the cluster split: persistent writes
// from a follower forward through raft and reach every voter (including a
// late joiner, via raft replication — not the AP snapshot); when the origin
// node dies, its ephemeral instances are purged but the persistent one
// survives on the remaining nodes.
func TestClusterPersistentInstance(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.Addr())
	a3 := newClusterApp(t, t.TempDir(), a1.Addr())
	provider2 := newNaming(t, a2.Addr())
	consumer1 := newNaming(t, a1.Addr())
	consumer3 := newNaming(t, a3.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// a2 must be a follower so the registration exercises the forward path.
	var rs struct {
		State string `json:"state"`
	}
	adminGet(t, "http://"+a2.AdminAddr()+"/api/raft", &rs)
	if rs.State != "Follower" {
		t.Fatalf("a2 raft state = %s, want Follower", rs.State)
	}

	const svc = "cluster.Persist"
	_, port := livePort(t)
	persistent := nacos.Instance{IP: "127.0.0.1", Port: port, Weight: 1, Healthy: true, Enabled: true, Ephemeral: false}
	ephemeral := nacos.Instance{IP: "127.0.0.1", Port: port + 1, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider2.Register(ctx, svc, persistent); err != nil {
		t.Fatalf("register persistent via follower: %v", err)
	}
	if err := provider2.Register(ctx, svc, ephemeral); err != nil {
		t.Fatalf("register ephemeral on a2: %v", err)
	}
	// Every node sees both: persistent via raft apply, ephemeral via AP sync.
	waitQueryHosts(t, ctx, consumer1, svc, 2)
	waitQueryHosts(t, ctx, consumer3, svc, 2)

	// Kill the origin node: the ephemeral instance dies with it (15s ping
	// death), the persistent one stays — budget must outlast the death window.
	a2.HardStop()
	waitPersistentOnly := func(c *nacos.NamingClient) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			info, err := c.Query(ctx, svc)
			if err == nil && len(info.Hosts) == 1 && !info.Hosts[0].Ephemeral && info.Hosts[0].Port == port {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for persistent-only view (last=%+v err=%v)", info, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitPersistentOnly(consumer1)
	waitPersistentOnly(consumer3)
}