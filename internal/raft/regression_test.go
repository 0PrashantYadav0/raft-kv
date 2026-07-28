package raft_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prashant/raft-kv/internal/kv"
	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/storage"
	"github.com/prashant/raft-kv/internal/transport"
)

// TestSingleNodeProposeCommits guards the regression where a single-node cluster
// elected a leader but could never advance commitIndex (no peers to replicate
// to), so every Propose blocked forever.
func TestSingleNodeProposeCommits(t *testing.T) {
	h := newHarness(t, 1, 0)
	defer h.stopAll()
	h.waitLeader(2 * time.Second)

	if err := h.put("solo", "committed"); err != nil {
		t.Fatalf("single-node propose failed: %v", err)
	}
	if !h.waitConverged("solo", "committed", 2*time.Second) {
		t.Fatal("single-node write did not apply")
	}
}

// failAppendStorage fails AppendLog to simulate a disk error.
type failAppendStorage struct {
	*storage.Memory
	fail bool
}

func (f *failAppendStorage) AppendLog(e []raft.LogEntry) error {
	if f.fail {
		return errors.New("simulated disk failure")
	}
	return f.Memory.AppendLog(e)
}

// TestProposeFailsOnStorageError verifies the persistence-before-ack guarantee:
// if the leader cannot durably append an entry, Propose must return an error
// rather than silently proceed.
func TestProposeFailsOnStorageError(t *testing.T) {
	fs := &failAppendStorage{Memory: storage.NewMemory()}
	net := transport.NewNetwork()
	fsm := kv.NewStore()

	node, err := raft.NewNode(raft.Config{
		ID:                 "n1",
		Peers:              []raft.NodeID{"n1"},
		Transport:          net.Transport("n1"),
		Storage:            fs,
		FSM:                fsm,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	net.Register("n1", node)
	node.Start()
	defer node.Stop()

	// Wait for leadership (the no-op append may fail harmlessly; leadership is
	// still assumed).
	deadline := time.Now().Add(2 * time.Second)
	for !node.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	fs.fail = true
	cmd, _ := kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("v")}.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := node.Propose(ctx, cmd); err == nil {
		t.Fatal("expected Propose to fail when storage.AppendLog errors")
	}
}

func TestConfigValidation(t *testing.T) {
	base := func() raft.Config {
		return raft.Config{
			ID:        "n1",
			Peers:     []raft.NodeID{"n1", "n2", "n3"},
			Transport: transport.NewNetwork().Transport("n1"),
			Storage:   storage.NewMemory(),
			FSM:       kv.NewStore(),
		}
	}
	cases := map[string]func(*raft.Config){
		"missing id":         func(c *raft.Config) { c.ID = "" },
		"peers without self": func(c *raft.Config) { c.Peers = []raft.NodeID{"n2", "n3"} },
		"duplicate peers":    func(c *raft.Config) { c.Peers = []raft.NodeID{"n1", "n2", "n2"} },
		"empty peers":        func(c *raft.Config) { c.Peers = nil },
		"bad election timeouts": func(c *raft.Config) {
			c.ElectionTimeoutMin = 300 * time.Millisecond
			c.ElectionTimeoutMax = 100 * time.Millisecond
		},
		"heartbeat too large": func(c *raft.Config) { c.HeartbeatInterval = 500 * time.Millisecond },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			mutate(&cfg)
			if _, err := raft.NewNode(cfg); err == nil {
				t.Fatalf("expected NewNode to reject config: %s", name)
			}
		})
	}
}
