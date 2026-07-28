// Package storage provides durable and in-memory implementations of the
// raft.Storage interface.
package storage

import (
	"errors"
	"sync"

	"github.com/prashant/raft-kv/internal/raft"
)

// ErrNotFound is returned when a requested log range is unavailable.
var ErrNotFound = errors.New("storage: entries not found")

// Memory is an in-memory raft.Storage, useful for tests and single-run demos.
// It is safe for concurrent use but provides no durability.
type Memory struct {
	mu        sync.Mutex
	term      uint64
	votedFor  raft.NodeID
	entries   []raft.LogEntry // contiguous, sorted by Index
	snapIndex uint64
	snapTerm  uint64
	snapshot  []byte
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{} }

func (m *Memory) SaveState(term uint64, votedFor raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.votedFor = votedFor
	return nil
}

func (m *Memory) LoadState() (uint64, raft.NodeID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, nil
}

func (m *Memory) AppendLog(entries []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *Memory) TruncateSuffix(from uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	keep := m.entries[:0:0]
	for _, e := range m.entries {
		if e.Index < from {
			keep = append(keep, e)
		}
	}
	m.entries = keep
	return nil
}

func (m *Memory) TruncatePrefix(upto uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	keep := m.entries[:0:0]
	for _, e := range m.entries {
		if e.Index > upto {
			keep = append(keep, e)
		}
	}
	m.entries = keep
	return nil
}

func (m *Memory) Entries(lo, hi uint64) ([]raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []raft.LogEntry
	for _, e := range m.entries {
		if e.Index >= lo && e.Index <= hi {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *Memory) FirstIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return m.snapIndex + 1, nil
	}
	return m.entries[0].Index, nil
}

func (m *Memory) LastIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return m.snapIndex, nil
	}
	return m.entries[len(m.entries)-1].Index, nil
}

func (m *Memory) SaveSnapshot(lastIndex, lastTerm uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapIndex = lastIndex
	m.snapTerm = lastTerm
	m.snapshot = append([]byte(nil), data...)
	return nil
}

func (m *Memory) LoadSnapshot() (uint64, uint64, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapIndex, m.snapTerm, m.snapshot, nil
}

func (m *Memory) Close() error { return nil }
