package config

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// CasConflictMessage is the exact sentence Nacos produces when a casMd5 no
// longer matches (ConfigOperationService:145-147). It travels with
// resultCode=500/errorCode=500 — the message is the only conflict signal.
const CasConflictMessage = "Cas publish fail, server md5 may have changed."

// Command is one replicated config write. The leader stamps Now so every
// replica applies identical bytes; CAS is evaluated inside Apply so the
// decision is deterministic across the cluster.
type Command struct {
	Op      string `json:"op"` // "publish" | "remove"
	Key     string `json:"key"`
	Content string `json:"content,omitempty"`
	CasMD5  string `json:"casMd5,omitempty"`
	Type    string `json:"type,omitempty"`
	AppName string `json:"appName,omitempty"`
	Now     int64  `json:"now"`
}

// ApplyResult reports what Apply did, so the leader can answer the client.
type ApplyResult struct {
	OK       bool
	Conflict bool
	Changed  bool // content actually changed (gates notify, like updateMd5)
	Message  string
}

// FSM is the replicated config state: an in-memory map for reads, backed by
// synchronous bbolt writes. raft owns ordering; the FSM owns the data.
type FSM struct {
	mu       sync.RWMutex
	items    map[string]Item
	store    *Store
	onChange func(key string)
}

func NewFSM(store *Store) (*FSM, error) {
	items, err := store.LoadAll()
	if err != nil {
		return nil, err
	}
	return &FSM{items: items, store: store}, nil
}

// NotifyOnChange installs the change hook: every applied write that changed
// state calls it with the key — on EVERY node, so each notifies its own
// listeners (cluster-wide config push with no extra RPCs).
func (f *FSM) NotifyOnChange(fn func(key string)) {
	f.mu.Lock()
	f.onChange = fn
	f.mu.Unlock()
}

func (f *FSM) Get(key string) (Item, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	it, ok := f.items[key]
	return it, ok
}

func (f *FSM) Apply(l *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return ApplyResult{Message: fmt.Sprintf("config: undecodable command: %v", err)}
	}
	f.mu.Lock()
	res, changedKey := f.applyLocked(cmd)
	hook := f.onChange
	f.mu.Unlock()
	// Outside the lock: the hook takes the Service's listen lock, and Listen
	// holds that lock while reading the FSM — calling back inside would
	// invert the order.
	if changedKey != "" && hook != nil {
		hook(changedKey)
	}
	return res
}

func (f *FSM) applyLocked(cmd Command) (ApplyResult, string) {
	switch cmd.Op {
	case "publish":
		cur, exists := f.items[cmd.Key]
		if cmd.CasMD5 != "" && exists && cur.MD5 != cmd.CasMD5 {
			return ApplyResult{Conflict: true, Message: CasConflictMessage}, ""
		}
		// A CAS publish on a missing key creates it (Nacos AbstractCAS).
		newMD5 := md5Hex(cmd.Content)
		it := Item{
			Content:      cmd.Content,
			MD5:          newMD5,
			Type:         cmd.Type,
			AppName:      cmd.AppName,
			LastModified: cmd.Now,
		}
		if err := f.store.Put(cmd.Key, it); err != nil {
			return ApplyResult{Message: fmt.Sprintf("config: store put: %v", err)}, ""
		}
		changed := !exists || cur.MD5 != newMD5
		f.items[cmd.Key] = it
		if changed {
			return ApplyResult{OK: true, Changed: true}, cmd.Key
		}
		return ApplyResult{OK: true}, ""

	case "remove":
		if _, exists := f.items[cmd.Key]; !exists {
			// Idempotent, and still a change event: Nacos fires on remove
			// regardless, so watchers of an absent key learn it stays absent.
			return ApplyResult{OK: true, Changed: true}, cmd.Key
		}
		if err := f.store.Delete(cmd.Key); err != nil {
			return ApplyResult{Message: fmt.Sprintf("config: store delete: %v", err)}, ""
		}
		delete(f.items, cmd.Key)
		return ApplyResult{OK: true, Changed: true}, cmd.Key

	default:
		return ApplyResult{Message: fmt.Sprintf("config: unknown op %q", cmd.Op)}, ""
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	items := make(map[string]Item, len(f.items))
	for k, v := range f.items {
		items[k] = v
	}
	return &fsmSnapshot{items: items}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var items map[string]Item
	if err := json.NewDecoder(rc).Decode(&items); err != nil {
		return err
	}
	if err := f.store.ReplaceAll(items); err != nil {
		return err
	}
	f.mu.Lock()
	f.items = items
	f.mu.Unlock()
	return nil
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

type fsmSnapshot struct {
	items map[string]Item
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.items)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// All returns a copy of the full state for read-only inspection (admin).
func (f *FSM) All() map[string]Item {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]Item, len(f.items))
	for k, v := range f.items {
		out[k] = v
	}
	return out
}
