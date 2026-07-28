package storage

import (
	"path/filepath"
	"testing"

	"github.com/prashant/raft-kv/internal/raft"
)

func TestBoltLogAppendAndRead(t *testing.T) {
	s := openTemp(t)

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	}
	if err := s.AppendLog(entries); err != nil {
		t.Fatal(err)
	}

	if first, _ := s.FirstIndex(); first != 1 {
		t.Fatalf("first index = %d", first)
	}
	if last, _ := s.LastIndex(); last != 3 {
		t.Fatalf("last index = %d", last)
	}
	got, err := s.Entries(1, 3)
	if err != nil || len(got) != 3 || string(got[2].Command) != "c" {
		t.Fatalf("entries = %v err=%v", got, err)
	}
}

func TestBoltTruncation(t *testing.T) {
	s := openTemp(t)
	for i := uint64(1); i <= 5; i++ {
		_ = s.AppendLog([]raft.LogEntry{{Term: 1, Index: i}})
	}
	if err := s.TruncateSuffix(4); err != nil {
		t.Fatal(err)
	}
	if last, _ := s.LastIndex(); last != 3 {
		t.Fatalf("after suffix truncate, last = %d", last)
	}
	if err := s.TruncatePrefix(1); err != nil {
		t.Fatal(err)
	}
	if first, _ := s.FirstIndex(); first != 2 {
		t.Fatalf("after prefix truncate, first = %d", first)
	}
}

func TestBoltStateAndSnapshotPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	s, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(7, "n2"); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(10, 3, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// Reopen and verify durability.
	s2, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	term, vf, _ := s2.LoadState()
	if term != 7 || vf != "n2" {
		t.Fatalf("state not durable: term=%d votedFor=%s", term, vf)
	}
	idx, tm, data, _ := s2.LoadSnapshot()
	if idx != 10 || tm != 3 || string(data) != "snap" {
		t.Fatalf("snapshot not durable: %d %d %q", idx, tm, data)
	}
}

func openTemp(t *testing.T) *Bolt {
	t.Helper()
	s, err := OpenBolt(filepath.Join(t.TempDir(), "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
