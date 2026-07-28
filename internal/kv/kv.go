// Package kv implements the replicated key-value state machine that sits on top
// of Raft. It satisfies raft.FSM: every committed command is applied here in the
// same order on every node, guaranteeing all replicas converge to identical
// state.
package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// Op is a mutation kind.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
)

// Command is the serialized form of a write applied through Raft.
type Command struct {
	Op    Op
	Key   string
	Value []byte
}

// Encode marshals a command for the Raft log.
func (c Command) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeCommand unmarshals a command from the Raft log.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&c)
	return c, err
}

// Store is an in-memory key-value FSM. Reads take a read lock; Apply (writes)
// takes a write lock. Apply is only ever called by Raft's single apply loop.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Apply decodes and applies a committed command. The returned value is the
// previous value for Put/Delete (nil if absent). It never panics on bad input;
// a malformed command is logged as an error result.
func (s *Store) Apply(cmd []byte) any {
	c, err := DecodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("kv: decode command: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.data[c.Key]
	switch c.Op {
	case OpPut:
		s.data[c.Key] = append([]byte(nil), c.Value...)
	case OpDelete:
		delete(s.data, c.Key)
	default:
		return fmt.Errorf("kv: unknown op %d", c.Op)
	}
	return prev
}

// Get returns the value for key and whether it exists. Safe for concurrent use
// with Apply because reads are linearizable only when routed through the leader;
// this method returns the local replica's view.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Len returns the number of keys (useful for tests and metrics).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot serializes the entire keyspace.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore replaces the keyspace from a snapshot produced by Snapshot.
func (s *Store) Restore(snapshot []byte) error {
	data := make(map[string][]byte)
	if len(snapshot) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&data); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}
