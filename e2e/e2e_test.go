// Package e2e drives go-nacos with sweet's hand-written Nacos client — the
// same client verified against a live Nacos 3.2.3, so a pass here is
// wire-compatibility evidence, not a self-graded mock.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/javahongxi/go-nacos/internal/app"
	"github.com/javahongxi/sweet/nacos"
)

func newApp(t *testing.T, dir string, tune ...func(*app.Options)) *app.App {
	t.Helper()
	opts := app.Options{
		GRPCListen:  "127.0.0.1:0",
		RaftListen:  "127.0.0.1:0",
		AdminListen: "127.0.0.1:0",
		DataDir:     dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, f := range tune {
		f(&opts)
	}
	a, err := app.New(opts)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	if err := a.Start(); err != nil {
		a.Stop()
		t.Fatalf("app.Start: %v", err)
	}
	t.Cleanup(a.Stop)
	return a
}

func newConfig(t *testing.T, addr string) *nacos.ConfigClient {
	t.Helper()
	c, err := nacos.NewConfigClient(nacos.ConfigOptions{Addr: addr, ReconnectBackoff: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	waitReady(t, c)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newNaming(t *testing.T, addr string) *nacos.NamingClient {
	t.Helper()
	c, err := nacos.NewNamingClient(nacos.NamingOptions{Addr: addr, ReconnectBackoff: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewNamingClient: %v", err)
	}
	waitReady(t, c)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitReady(t *testing.T, c interface{ WaitReady(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

// pinnedGRPCListen reserves a free port so a restarted node can reuse the
// same gRPC address (and thus the same cluster identity).
func pinnedGRPCListen(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve grpc port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitReconnect(t *testing.T, c interface {
	Handshakes() int64
	WaitReady(context.Context) error
}) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.Handshakes() > 1 {
			waitReady(t, c)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("client did not reconnect within 10s")
}

func TestConfigLifecycle(t *testing.T) {
	a := newApp(t, t.TempDir())
	c := newConfig(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "app.yaml", Group: "DEFAULT_GROUP"}
	if err := c.Publish(ctx, key, nacos.PublishInput{Content: "v1", Type: "yaml"}); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	got, err := c.Query(ctx, key)
	if err != nil {
		t.Fatalf("query v1: %v", err)
	}
	if got.Content != "v1" || got.MD5 == "" {
		t.Fatalf("query v1 = %+v", got)
	}

	updates := make(chan nacos.Config, 8)
	if err := c.Watch(ctx, key, func(cfg nacos.Config) { updates <- cfg }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	select {
	case seed := <-updates:
		if seed.Content != "v1" {
			t.Fatalf("watch seed = %q, want v1", seed.Content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch seed never arrived")
	}

	if err := c.Publish(ctx, key, nacos.PublishInput{Content: "v2"}); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	select {
	case upd := <-updates:
		if upd.Content != "v2" {
			t.Fatalf("watch update = %q, want v2", upd.Content)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("watch update never arrived")
	}

	// CAS with the stale v1 md5 must lose with ErrConflict.
	err = c.Publish(ctx, key, nacos.PublishInput{Content: "v3", ExpectMD5: got.MD5})
	if !errors.Is(err, nacos.ErrConflict) {
		t.Fatalf("stale CAS publish: err = %v, want ErrConflict", err)
	}

	if err := c.Remove(ctx, key); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := c.Query(ctx, key); !errors.Is(err, nacos.ErrNotFound) {
		t.Fatalf("query after remove: err = %v, want ErrNotFound", err)
	}
}

func TestConfigPersistence(t *testing.T) {
	dir := t.TempDir()
	// Node identity IS the gRPC address, so a restart keeps it only on a
	// fixed port — exactly the production setup.
	grpcAddr := pinnedGRPCListen(t)
	pin := func(o *app.Options) { o.GRPCListen = grpcAddr }
	a := newApp(t, dir, pin)
	c := newConfig(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "persist.yaml", Group: "DEFAULT_GROUP"}
	if err := c.Publish(ctx, key, nacos.PublishInput{Content: "durable"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	c.Close()
	a.Stop()

	a2 := newApp(t, dir, pin)
	c2 := newConfig(t, a2.Addr())
	got, err := c2.Query(ctx, key)
	if err != nil {
		t.Fatalf("query after restart: %v", err)
	}
	if got.Content != "durable" {
		t.Fatalf("after restart content = %q, want durable", got.Content)
	}
}

func TestNamingLifecycle(t *testing.T) {
	a := newApp(t, t.TempDir())
	provider := newNaming(t, a.Addr())
	consumer := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const svc = "greeter.Greeter"
	inst1 := nacos.Instance{IP: "127.0.0.1", Port: 20000, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	inst2 := nacos.Instance{IP: "127.0.0.1", Port: 20001, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}

	if err := provider.Register(ctx, svc, inst1); err != nil {
		t.Fatalf("register inst1: %v", err)
	}
	info, err := consumer.Subscribe(ctx, svc)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if len(info.Hosts) != 1 {
		t.Fatalf("subscribe snapshot hosts = %d, want 1", len(info.Hosts))
	}

	if err := provider.Register(ctx, svc, inst2); err != nil {
		t.Fatalf("register inst2: %v", err)
	}
	waitHosts(t, consumer.Updates(), 2)

	if err := provider.Deregister(ctx, svc, inst2); err != nil {
		t.Fatalf("deregister inst2: %v", err)
	}
	waitHosts(t, consumer.Updates(), 1)

	// Closing the provider connection must drop its instance: liveness IS the
	// connection.
	provider.Close()
	waitHosts(t, consumer.Updates(), 0)
}

func TestNamingReconnectReplay(t *testing.T) {
	a := newApp(t, t.TempDir())
	provider := newNaming(t, a.Addr())
	consumer := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const svc = "greeter.Greeter"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 20000, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	a.DropConnections()
	waitReconnect(t, provider)
	waitReconnect(t, consumer)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		info, err := consumer.Query(ctx, svc)
		if err == nil && len(info.Hosts) == 1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("instance never came back after reconnect replay")
}

func TestConfigReconnectReplay(t *testing.T) {
	a := newApp(t, t.TempDir())
	watcher := newConfig(t, a.Addr())
	publisher := newConfig(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "replay.yaml", Group: "DEFAULT_GROUP"}
	updates := make(chan nacos.Config, 8)
	// Watching a key that does not exist yet: the listen must survive the
	// reconnect so the first publish reaches the watcher.
	if err := watcher.Watch(ctx, key, func(cfg nacos.Config) { updates <- cfg }); err != nil {
		t.Fatalf("watch: %v", err)
	}

	a.DropConnections()
	waitReconnect(t, watcher)
	waitReconnect(t, publisher)

	if err := publisher.Publish(ctx, key, nacos.PublishInput{Content: "after-replay"}); err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}
	select {
	case upd := <-updates:
		if upd.Content != "after-replay" {
			t.Fatalf("watch update = %q, want after-replay", upd.Content)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watch update never arrived after reconnect")
	}
}

func waitHosts(t *testing.T, updates <-chan nacos.ServiceInfo, want int) {
	t.Helper()
	deadline := time.After(8 * time.Second)
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

func TestAdminReadOnly(t *testing.T) {
	a := newApp(t, t.TempDir())
	cfg := newConfig(t, a.Addr())
	provider := newNaming(t, a.Addr())
	consumer := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := nacos.ConfigKey{DataID: "admin.yaml", Group: "DEFAULT_GROUP"}
	if err := cfg.Publish(ctx, key, nacos.PublishInput{Content: "hello-admin", Type: "yaml"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	const svc = "admin.Svc"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 21000, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	base := "http://" + a.AdminAddr()

	var ov struct {
		Configs     int    `json:"configs"`
		Services    int    `json:"services"`
		Connections int    `json:"connections"`
		RaftState   string `json:"raftState"`
	}
	adminGet(t, base+"/api/overview", &ov)
	if ov.Configs < 1 || ov.Services < 1 || ov.Connections < 1 || ov.RaftState != "Leader" {
		t.Fatalf("overview = %+v", ov)
	}

	var configs []struct {
		DataID string `json:"dataId"`
		MD5    string `json:"md5"`
	}
	adminGet(t, base+"/api/configs", &configs)
	found := false
	for _, c := range configs {
		if c.DataID == "admin.yaml" && c.MD5 != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("configs missing admin.yaml: %+v", configs)
	}

	var detail struct {
		Content string `json:"content"`
		MD5     string `json:"md5"`
	}
	adminGet(t, base+"/api/config?dataId=admin.yaml&group=DEFAULT_GROUP&tenant=public", &detail)
	if detail.Content != "hello-admin" || detail.MD5 == "" {
		t.Fatalf("config detail = %+v", detail)
	}

	var services []struct {
		Name        string `json:"name"`
		Instances   int    `json:"instances"`
		Subscribers int    `json:"subscribers"`
	}
	adminGet(t, base+"/api/services", &services)
	found = false
	for _, s := range services {
		if s.Name == svc && s.Instances == 1 && s.Subscribers == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("services missing %s with 1 instance/1 subscriber: %+v", svc, services)
	}

	var sd struct {
		ServiceInfo struct {
			Hosts []struct {
				IP   string `json:"ip"`
				Port int    `json:"port"`
			} `json:"hosts"`
		} `json:"serviceInfo"`
		Subscribers []string `json:"subscribers"`
	}
	adminGet(t, base+"/api/service?name=admin.Svc&group=DEFAULT_GROUP&namespace=public", &sd)
	if len(sd.ServiceInfo.Hosts) != 1 || sd.ServiceInfo.Hosts[0].Port != 21000 || len(sd.Subscribers) != 1 {
		t.Fatalf("service detail = %+v", sd)
	}

	var conns []struct {
		ID     string `json:"id"`
		Module string `json:"module"`
	}
	adminGet(t, base+"/api/connections", &conns)
	if len(conns) == 0 {
		t.Fatal("connections empty")
	}

	var raftStatus struct {
		State string            `json:"state"`
		Stats map[string]string `json:"stats"`
	}
	adminGet(t, base+"/api/raft", &raftStatus)
	if raftStatus.State != "Leader" || raftStatus.Stats["term"] == "" {
		t.Fatalf("raft = %+v", raftStatus)
	}

	// The embedded frontend is served at /.
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "go-nacos console") {
		t.Fatalf("frontend status=%d, body lacks title", resp.StatusCode)
	}

	// The list route stays GET-only: a write method is rejected with 405.
	pr, err := http.Post(base+"/api/configs", "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	pr.Body.Close()
	if pr.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/configs status=%d, want 405", pr.StatusCode)
	}
}

func adminGet(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s status=%d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

// TestNamingEmptyServiceClean locks the Java-aligned cleaner semantics: the
// empty shell is dropped, the subscriber index survives, and a later
// registration still pushes to the ORIGINAL subscriber (no reconnect).
func TestNamingEmptyServiceClean(t *testing.T) {
	a := newApp(t, t.TempDir(), func(o *app.Options) {
		o.NamingCleanExpire = 100 * time.Millisecond
		o.NamingCleanInterval = 50 * time.Millisecond
	})
	provider := newNaming(t, a.Addr())
	consumer := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const svc = "clean.Svc"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 22000, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Provider dies: the instance goes, the consumer gets the empty push.
	provider.Close()
	waitHosts(t, consumer.Updates(), 0)

	// The cleaner drops the empty shell but must keep the subscriber index.
	base := "http://" + a.AdminAddr()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var services []struct {
			Name string `json:"name"`
		}
		adminGet(t, base+"/api/services", &services)
		gone := true
		for _, s := range services {
			if s.Name == svc {
				gone = false
			}
		}
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("empty service shell was never cleaned")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var sd struct {
		ServiceInfo struct {
			Hosts []struct {
				IP string `json:"ip"`
			} `json:"hosts"`
		} `json:"serviceInfo"`
		Subscribers []string `json:"subscribers"`
	}
	adminGet(t, base+"/api/service?name="+svc+"&group=DEFAULT_GROUP&namespace=public", &sd)
	if len(sd.ServiceInfo.Hosts) != 0 || len(sd.Subscribers) != 1 {
		t.Fatalf("after clean: hosts=%d subscribers=%d, want 0/1", len(sd.ServiceInfo.Hosts), len(sd.Subscribers))
	}

	// A brand-new provider registers the same service: the ORIGINAL consumer,
	// still on its first connection, must receive the push.
	provider2 := newNaming(t, a.Addr())
	if err := provider2.Register(ctx, svc, inst); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	waitHosts(t, consumer.Updates(), 1)
	if h := consumer.Handshakes(); h != 1 {
		t.Fatalf("consumer handshakes = %d, want 1 (no reconnect)", h)
	}
}

// TestAdminWrites proves console writes flow through the same domain paths as
// the SDK: raft-backed config writes are visible to SDK queries and watchers,
// CAS conflicts surface as 409, and naming overrides never touch connections.
func TestAdminWrites(t *testing.T) {
	a := newApp(t, t.TempDir())
	cfg := newConfig(t, a.Addr())
	provider := newNaming(t, a.Addr())
	consumer := newNaming(t, a.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base := "http://" + a.AdminAddr()

	// 1. Create via admin; the SDK must see it (same raft path).
	adminWrite(t, http.MethodPost, base+"/api/config", map[string]any{
		"dataId": "w.yaml", "group": "DEFAULT_GROUP", "tenant": "public", "content": "v1", "type": "yaml",
	}, http.StatusOK)
	key := nacos.ConfigKey{DataID: "w.yaml", Group: "DEFAULT_GROUP"}
	got, err := cfg.Query(ctx, key)
	if err != nil || got.Content != "v1" || got.MD5 == "" {
		t.Fatalf("sdk query after admin publish: %+v err=%v", got, err)
	}

	// 2. Admin edit with CAS must reach the SDK watcher (notify path).
	updates := make(chan nacos.Config, 8)
	if err := cfg.Watch(ctx, key, func(c nacos.Config) { updates <- c }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	select {
	case seed := <-updates:
		if seed.Content != "v1" {
			t.Fatalf("watch seed = %q, want v1", seed.Content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch seed never arrived")
	}
	adminWrite(t, http.MethodPost, base+"/api/config", map[string]any{
		"dataId": "w.yaml", "content": "v2", "expectMd5": got.MD5,
	}, http.StatusOK)
	select {
	case upd := <-updates:
		if upd.Content != "v2" {
			t.Fatalf("watch update = %q, want v2", upd.Content)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("admin edit never reached the SDK watcher")
	}

	// 3. Stale CAS loses with 409.
	adminWrite(t, http.MethodPost, base+"/api/config", map[string]any{
		"dataId": "w.yaml", "content": "v3", "expectMd5": got.MD5,
	}, http.StatusConflict)

	// 4. Admin delete → SDK ErrNotFound.
	adminWrite(t, http.MethodDelete, base+"/api/config?dataId=w.yaml&group=DEFAULT_GROUP&tenant=public", nil, http.StatusOK)
	if _, err := cfg.Query(ctx, key); !errors.Is(err, nacos.ErrNotFound) {
		t.Fatalf("query after admin delete: err = %v, want ErrNotFound", err)
	}

	// 5. Force-offline an instance without disconnecting its provider.
	const svc = "write.Svc"
	inst := nacos.Instance{IP: "127.0.0.1", Port: 23000, Weight: 1, Healthy: true, Enabled: true, Ephemeral: true}
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := consumer.Subscribe(ctx, svc); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	instURL := base + "/api/instance?name=write.Svc&group=DEFAULT_GROUP&namespace=public&ip=127.0.0.1&port=23000&cluster=DEFAULT"
	adminWrite(t, http.MethodDelete, instURL, nil, http.StatusOK)
	waitHosts(t, consumer.Updates(), 0)
	if h := provider.Handshakes(); h != 1 {
		t.Fatalf("provider handshakes = %d, want 1 (still connected)", h)
	}

	// 6. Service delete guards: non-empty → 409; empty → 200; subscribers survive.
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	waitHosts(t, consumer.Updates(), 1)
	svcURL := base + "/api/service?name=write.Svc&group=DEFAULT_GROUP&namespace=public"
	adminWrite(t, http.MethodDelete, svcURL, nil, http.StatusConflict)
	adminWrite(t, http.MethodDelete, instURL, nil, http.StatusOK)
	waitHosts(t, consumer.Updates(), 0)
	adminWrite(t, http.MethodDelete, svcURL, nil, http.StatusOK)
	var services []struct {
		Name string `json:"name"`
	}
	adminGet(t, base+"/api/services", &services)
	for _, s := range services {
		if s.Name == svc {
			t.Fatal("service still listed after delete")
		}
	}
	// The original consumer (no reconnect) still gets the push on re-register.
	if err := provider.Register(ctx, svc, inst); err != nil {
		t.Fatalf("re-register after delete: %v", err)
	}
	waitHosts(t, consumer.Updates(), 1)
	if h := consumer.Handshakes(); h != 1 {
		t.Fatalf("consumer handshakes = %d, want 1 (no reconnect)", h)
	}
}

func adminWrite(t *testing.T, method, url string, body any, wantStatus int) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s status=%d, want %d (body=%s)", method, url, resp.StatusCode, wantStatus, msg)
	}
}
