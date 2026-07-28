package raft_test

import (
	"testing"
	"time"

	"github.com/prashant/raft-kv/internal/kv"
	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/storage"
	"github.com/prashant/raft-kv/internal/transport"
)

// TestAppendEntriesRejectsCompactedPrev ensures a follower whose log has been
// compacted past PrevLogIndex REJECTS the append (so the leader backs up and
// retries) rather than acking entries it never stored — which would inflate the
// leader's matchIndex and risk a lost write.
func TestAppendEntriesRejectsCompactedPrev(t *testing.T) {
	net := transport.NewNetwork()
	fsm := kv.NewStore()
	node, err := raft.NewNode(raft.Config{
		ID:                 "n1",
		Peers:              []raft.NodeID{"n1", "n2", "n3"},
		Transport:          net.Transport("n1"),
		Storage:            storage.NewMemory(),
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

	// Install a snapshot up to index 10 so the log is compacted below 10.
	snap, _ := fsm.Snapshot()
	node.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 2, LeaderID: "n2", LastIncludedIndex: 10, LastIncludedTerm: 2,
		Data: snap, Done: true,
	})

	// AppendEntries referencing a compacted PrevLogIndex must be rejected.
	reply := node.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: "n2", PrevLogIndex: 5, PrevLogTerm: 1,
		Entries: []raft.LogEntry{{Term: 2, Index: 6}},
	})
	if reply.Success {
		t.Fatal("append with compacted PrevLogIndex must NOT be acked")
	}
	if reply.ConflictIndex != 11 {
		t.Fatalf("expected ConflictIndex=11 (snapshotIndex+1), got %d", reply.ConflictIndex)
	}
}
