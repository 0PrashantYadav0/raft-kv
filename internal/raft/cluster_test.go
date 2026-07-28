package raft_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prashant/raft-kv/internal/kv"
	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/storage"
	"github.com/prashant/raft-kv/internal/transport"
)

// harness spins up an in-memory Raft cluster wired over transport.Network.
type harness struct {
	t      *testing.T
	net    *transport.Network
	nodes  map[raft.NodeID]*raft.Node
	stores map[raft.NodeID]*kv.Store
	stg    map[raft.NodeID]*storage.Memory
	ids    []raft.NodeID
	snap   uint64
}

func newHarness(t *testing.T, n int, snapThreshold uint64) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		net:    transport.NewNetwork(),
		nodes:  make(map[raft.NodeID]*raft.Node),
		stores: make(map[raft.NodeID]*kv.Store),
		stg:    make(map[raft.NodeID]*storage.Memory),
		snap:   snapThreshold,
	}
	for i := 0; i < n; i++ {
		h.ids = append(h.ids, raft.NodeID(fmt.Sprintf("n%d", i+1)))
	}
	for _, id := range h.ids {
		h.start(id)
	}
	return h
}

func (h *harness) start(id raft.NodeID) {
	h.t.Helper()
	stg := h.stg[id]
	if stg == nil {
		stg = storage.NewMemory()
		h.stg[id] = stg
	}
	store := h.stores[id]
	if store == nil {
		store = kv.NewStore()
		h.stores[id] = store
	}
	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              h.ids,
		Transport:          h.net.Transport(id),
		Storage:            stg,
		FSM:                store,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		SnapshotThreshold:  h.snap,
	})
	if err != nil {
		h.t.Fatalf("new node %s: %v", id, err)
	}
	h.net.Register(id, node)
	h.nodes[id] = node
	node.Start()
}

func (h *harness) stopAll() {
	for _, n := range h.nodes {
		n.Stop()
	}
}

// kill stops a node and partitions it away, removing it from the active set so
// harness helpers ignore it. Its durable storage is retained for restart.
func (h *harness) kill(id raft.NodeID) {
	h.net.SetDown(id, true)
	if n, ok := h.nodes[id]; ok {
		n.Stop()
	}
	delete(h.nodes, id)
	delete(h.stores, id)
}

// restart brings a previously killed node back using its retained storage.
func (h *harness) restart(id raft.NodeID) {
	h.net.SetDown(id, false)
	h.start(id)
}

// waitLeader polls until exactly one leader emerges (or the deadline passes).
func (h *harness) waitLeader(timeout time.Duration) raft.NodeID {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := h.leaders()
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("no single leader elected within %s (found %v)", timeout, h.leaders())
	return ""
}

func (h *harness) leaders() []raft.NodeID {
	var out []raft.NodeID
	for id, n := range h.nodes {
		if n.IsLeader() {
			out = append(out, id)
		}
	}
	return out
}

// put proposes a Put through whichever node is currently leader.
func (h *harness) put(key, value string) error {
	cmd, err := kv.Command{Op: kv.OpPut, Key: key, Value: []byte(value)}.Encode()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range h.nodes {
			if !n.IsLeader() {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := n.Propose(ctx, cmd)
			cancel()
			if err == nil {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("put(%s) never committed", key)
}

// waitConverged waits until every live store has the expected value for key.
func (h *harness) waitConverged(key, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, s := range h.stores {
			v, ok := s.Get(key)
			if !ok || string(v) != want {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
