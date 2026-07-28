package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/prashant/raft-kv/internal/raft"
)

func TestSingleNodeBecomesLeader(t *testing.T) {
	h := newHarness(t, 1, 0)
	defer h.stopAll()
	leader := h.waitLeader(2 * time.Second)
	if leader != "n1" {
		t.Fatalf("expected n1 to lead, got %s", leader)
	}
}

func TestThreeNodeElectsSingleLeader(t *testing.T) {
	h := newHarness(t, 3, 0)
	defer h.stopAll()
	h.waitLeader(2 * time.Second)
	if got := len(h.leaders()); got != 1 {
		t.Fatalf("expected exactly one leader, got %d", got)
	}
}

func TestLogReplicationConverges(t *testing.T) {
	h := newHarness(t, 3, 0)
	defer h.stopAll()
	h.waitLeader(2 * time.Second)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := h.put(key, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		if !h.waitConverged(key, fmt.Sprintf("v%d", i), 2*time.Second) {
			t.Fatalf("key %s did not converge on all replicas", key)
		}
	}
}

// TestLeaderFailoverNoDataLoss is the flagship scenario from the resume:
// kill the leader, confirm a new one is elected and no committed data is lost.
func TestLeaderFailoverNoDataLoss(t *testing.T) {
	h := newHarness(t, 5, 0)
	defer h.stopAll()

	leader := h.waitLeader(2 * time.Second)
	if err := h.put("durable", "before"); err != nil {
		t.Fatalf("initial put: %v", err)
	}
	if !h.waitConverged("durable", "before", 2*time.Second) {
		t.Fatal("value did not replicate before failover")
	}

	// Roll the leader.
	h.kill(leader)

	newLeader := h.waitLeader(3 * time.Second)
	if newLeader == leader {
		t.Fatalf("new leader must differ from killed leader %s", leader)
	}

	// Previously committed data must survive, and new writes must proceed.
	if v, ok := h.stores[newLeader].Get("durable"); !ok || string(v) != "before" {
		t.Fatalf("committed data lost after failover: got %q ok=%v", v, ok)
	}
	if err := h.put("durable", "after"); err != nil {
		t.Fatalf("post-failover put: %v", err)
	}
	if !h.waitConverged("durable", "after", 2*time.Second) {
		t.Fatal("post-failover write did not converge")
	}
}

// TestRollingRestartKeepsData kills nodes one at a time (keeping a quorum) and
// verifies the cluster stays available and consistent throughout.
func TestRollingRestartKeepsData(t *testing.T) {
	h := newHarness(t, 5, 0)
	defer h.stopAll()
	h.waitLeader(2 * time.Second)

	if err := h.put("x", "1"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Kill two followers/leaders one at a time; 5-node cluster tolerates 2 down.
	victims := append([]raft.NodeID(nil), h.ids...)
	killed := 0
	for _, id := range victims {
		if killed >= 2 {
			break
		}
		h.kill(id)
		killed++
		h.waitLeader(3 * time.Second)
		val := fmt.Sprintf("v%d", killed)
		if err := h.put("x", val); err != nil {
			t.Fatalf("put after killing %s: %v", id, err)
		}
		if !h.waitConverged("x", val, 2*time.Second) {
			t.Fatalf("write did not converge after killing %s", id)
		}
	}
}

// TestSnapshotCatchUp forces snapshotting on a small threshold, then restarts a
// node so it must catch up. It exercises both log replay and (once compacted)
// InstallSnapshot.
func TestSnapshotCatchUp(t *testing.T) {
	h := newHarness(t, 3, 5) // snapshot every 5 applied entries
	defer h.stopAll()
	h.waitLeader(2 * time.Second)

	// Kill one node, then write enough entries to trigger compaction on the
	// survivors so the returning node needs a snapshot.
	h.kill("n3")
	for i := 0; i < 30; i++ {
		if err := h.put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Bring n3 back; it should catch up to the latest committed state.
	h.restart("n3")
	h.waitLeader(2 * time.Second)

	deadline := time.Now().Add(4 * time.Second)
	for {
		v, ok := h.stores["n3"].Get("k29")
		if ok && string(v) == "v29" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("n3 failed to catch up via snapshot/replication: got %q ok=%v", v, ok)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
