// Package transport contains the wire implementations of raft.Transport plus
// the gRPC server that exposes a node's RPC handlers to the cluster.
package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prashant/raft-kv/internal/raft"
)

// ErrUnreachable simulates a network failure to a peer.
var ErrUnreachable = errors.New("transport: peer unreachable")

// Network is an in-process message bus used by tests and the local demo. It can
// simulate partitions (SetDown) and per-message latency, which lets us exercise
// leader election and log catch-up deterministically without real sockets.
type Network struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]raft.RPCHandler
	down     map[raft.NodeID]bool
	latency  time.Duration
}

// NewNetwork returns an empty in-memory network.
func NewNetwork() *Network {
	return &Network{
		handlers: make(map[raft.NodeID]raft.RPCHandler),
		down:     make(map[raft.NodeID]bool),
	}
}

// Register wires a node's RPC handler into the network.
func (nw *Network) Register(id raft.NodeID, h raft.RPCHandler) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.handlers[id] = h
}

// SetDown marks a node reachable or unreachable (simulated partition).
func (nw *Network) SetDown(id raft.NodeID, down bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.down[id] = down
}

// SetLatency injects a fixed delay before each delivered RPC.
func (nw *Network) SetLatency(d time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.latency = d
}

// Transport returns a raft.Transport bound to the given source node.
func (nw *Network) Transport(from raft.NodeID) raft.Transport {
	return &memTransport{nw: nw, from: from}
}

func (nw *Network) deliver(from, to raft.NodeID) (raft.RPCHandler, error) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	if nw.down[from] || nw.down[to] {
		return nil, ErrUnreachable
	}
	h, ok := nw.handlers[to]
	if !ok {
		return nil, ErrUnreachable
	}
	if nw.latency > 0 {
		time.Sleep(nw.latency)
	}
	return h, nil
}

type memTransport struct {
	nw   *Network
	from raft.NodeID
}

func (t *memTransport) RequestVote(_ context.Context, target raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, err := t.nw.deliver(t.from, target)
	if err != nil {
		return nil, err
	}
	return h.HandleRequestVote(args), nil
}

func (t *memTransport) AppendEntries(_ context.Context, target raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, err := t.nw.deliver(t.from, target)
	if err != nil {
		return nil, err
	}
	return h.HandleAppendEntries(args), nil
}

func (t *memTransport) InstallSnapshot(_ context.Context, target raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	h, err := t.nw.deliver(t.from, target)
	if err != nil {
		return nil, err
	}
	return h.HandleInstallSnapshot(args), nil
}
