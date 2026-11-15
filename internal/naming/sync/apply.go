package sync

import (
	"encoding/json"
	"fmt"

	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/go-nacos/internal/wire"
)

// Message types (Metadata.type on the internal Cluster/sync RPC). Five
// shapes, JSON bodies — the r-nacos inventory.
const (
	TypeSyncInstances = "SyncInstances"
	TypeRemoveConn    = "RemoveConn"
	TypeFullMapping   = "FullMapping"
	TypePing          = "Ping"
	TypeSnapshot      = "Snapshot"
)

type instancesMsg struct {
	From   string        `json:"from"`
	Action string        `json:"action"` // register | deregister
	Key    naming.Key    `json:"key"`
	ConnID string        `json:"connId"`
	Inst   wire.Instance `json:"inst"`
}

type removeConnMsg struct {
	From   string `json:"from"`
	ConnID string `json:"connId"`
}

type fullMappingMsg struct {
	From  string                    `json:"from"`
	Conns map[string][]naming.Entry `json:"conns"`
}

type pingMsg struct {
	From string `json:"from"`
}

type snapshotMsg struct {
	Entries []naming.Entry `json:"entries"`
}

// HandleSync applies one inbound message (cluster.Syncer). Every mutation
// goes through the naming remote-apply path, which never re-emits: loop
// prevention by construction.
func (m *Manager) HandleSync(typeName string, data []byte) error {
	switch typeName {
	case TypePing:
		var msg pingMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		m.touch(msg.From)
		return nil

	case TypeSyncInstances:
		var msg instancesMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		m.touch(msg.From)
		switch msg.Action {
		case "register":
			m.svc.ApplyRemoteRegister(msg.From, msg.Key, msg.Inst, msg.ConnID)
		case "deregister":
			m.svc.ApplyRemoteDeregister(msg.From, msg.Key, msg.Inst, msg.ConnID)
		default:
			return fmt.Errorf("sync: unknown action %q", msg.Action)
		}
		return nil

	case TypeRemoveConn:
		var msg removeConnMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		m.touch(msg.From)
		m.svc.ApplyRemoteRemoveConn(msg.From, msg.ConnID)
		return nil

	case TypeFullMapping:
		var msg fullMappingMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		m.touch(msg.From)
		m.svc.Reconcile(msg.From, msg.Conns)
		return nil

	default:
		return fmt.Errorf("sync: unknown type %q", typeName)
	}
}

// Snapshot serves a full-state pull (cluster.Syncer): every entry with its
// true origin, so the puller keeps ownership attribution and death cleanup
// stays correct there too.
func (m *Manager) Snapshot() ([]byte, error) {
	return json.Marshal(snapshotMsg{Entries: m.svc.AllEntries()})
}