package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/javahongxi/go-nacos/internal/app"
	"github.com/javahongxi/sweet/nacos"
)

// newClusterApp starts one node: join == "" bootstraps a fresh cluster,
// otherwise the node joins through that peer's internal sync address.
func newClusterApp(t *testing.T, dir, join string) *app.App {
	t.Helper()
	return newApp(t, dir, func(o *app.Options) { o.JoinAddr = join })
}

// TestClusterConfigForwarding locks M2a: a follower forwards writes to the
// leader, every node serves reads, and the FSM change hook pushes to local
// watchers on BOTH nodes regardless of where the write landed.
func TestClusterConfigForwarding(t *testing.T) {
	a1 := newClusterApp(t, t.TempDir(), "")
	a2 := newClusterApp(t, t.TempDir(), a1.SyncAddr())
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