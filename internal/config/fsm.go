package config

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

// CasConflictMessage is the exact sentence Nacos produces when a casMd5 no
// longer matches (ConfigOperationService:145-147). It travels with
// resultCode=500/errorCode=500 — the message is the only conflict signal.
const CasConflictMessage = "Cas publish fail, server md5 may have changed."

// maxHistory caps the per-key version list (r-nacos ConfigValue.update_value:
// drop the oldest once 100 is reached). History lives in the FSM and rides
// the raft snapshot — there is no side database.
const maxHistory = 100

// Item is one stored config. MD5 is derived from Content at write time.
// History holds every retained version INCLUDING the current one, oldest
// first (r-nacos histories semantics); removing a config drops its history.
type Item struct {
	Content      string        `json:"content"`
	MD5          string        `json:"md5"`
	Type         string        `json:"type,omitempty"`
	AppName      string        `json:"appName,omitempty"`
	LastModified int64         `json:"lastModified"`
	History      []HistoryItem `json:"history,omitempty"`
}

// HistoryItem is one retained version of a config.
type HistoryItem struct {
	Content      string `json:"content"`
	MD5          string `json:"md5"`
	Type         string `json:"type,omitempty"`
	AppName      string `json:"appName,omitempty"`
	LastModified int64  `json:"lastModified"`
}

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

// FSM is the replicated config state: an in-memory map, durable through raft
// log replay and snapshots alone (r-nacos / jaws-harbor shape — the bbolt
// side copy is gone). raft owns ordering; the FSM owns the data.
type FSM struct {
	mu       sync.RWMutex
	items    map[string]Item
	onChange func(key string)
}

// NewFSM starts empty: startup recovery is raft's snapshot restore plus log
// replay, not a local load.
func NewFSM() *FSM {
	return &FSM{items: map[string]Item{}}
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

// History returns a copy of the key's retained versions, oldest first.
func (f *FSM) History(key string) []HistoryItem {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return append([]HistoryItem(nil), f.items[key].History...)
}

// ApplyData implements fsm.Domain: one decoded config command.
func (f *FSM) ApplyData(data []byte) any {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
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
			History:      cur.History,
		}
		changed := !exists || cur.MD5 != newMD5
		if changed {
			// Every retained version including the current one; an unchanged
			// republish adds no history (r-nacos returns early on equal md5).
			it.History = append(it.History, HistoryItem{
				Content: cmd.Content, MD5: newMD5, Type: cmd.Type, AppName: cmd.AppName, LastModified: cmd.Now,
			})
			if len(it.History) > maxHistory {
				it.History = it.History[len(it.History)-maxHistory:]
			}
		}
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
		delete(f.items, cmd.Key) // history goes with the config
		return ApplyResult{OK: true, Changed: true}, cmd.Key

	default:
		return ApplyResult{Message: fmt.Sprintf("config: unknown op %q", cmd.Op)}, ""
	}
}

// SnapshotData implements fsm.Domain: a copy of the full state (history
// included) for the composite raft snapshot.
func (f *FSM) SnapshotData() any {
	f.mu.RLock()
	defer f.mu.RUnlock()
	items := make(map[string]Item, len(f.items))
	for k, v := range f.items {
		items[k] = v
	}
	return items
}

// RestoreData implements fsm.Domain: swap in the snapshot state.
func (f *FSM) RestoreData(data []byte) error {
	var items map[string]Item
	if err := json.Unmarshal(data, &items); err != nil {
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