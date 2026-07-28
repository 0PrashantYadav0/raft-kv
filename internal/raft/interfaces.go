package raft

import "context"

// Transport is how a Raft node sends RPCs to its peers. It is deliberately
// small so it can be backed by anything: an in-memory pipe (tests), net/rpc,
// or gRPC (production). Implementations must be safe for concurrent use.
type Transport interface {
	RequestVote(ctx context.Context, target NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, target NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, target NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler is the server side of the Transport: the object that a transport
// server dispatches incoming RPCs to. A *Node satisfies this interface.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

// FSM is the finite state machine that Raft replicates. Apply is invoked for
// every committed log entry, in order, on every node. Snapshot/Restore support
// log compaction.
//
// Apply must be deterministic: given the same sequence of commands, every node
// must reach the same state. The returned value is surfaced to the local caller
// that proposed the command (it is never sent over the wire).
type FSM interface {
	Apply(cmd []byte) any
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

// Storage persists the pieces of Raft state that must survive a crash (§5):
// currentTerm, votedFor, and the log. It also stores the most recent snapshot.
// All methods must be durable before they return (fsync-on-write semantics).
type Storage interface {
	// SaveState persists currentTerm and votedFor atomically.
	SaveState(term uint64, votedFor NodeID) error
	// LoadState returns the persisted term and vote, or (0, "") on a fresh node.
	LoadState() (term uint64, votedFor NodeID, err error)

	// AppendEntries durably appends entries to the persistent log.
	AppendLog(entries []LogEntry) error
	// TruncateSuffix removes all entries with index >= from (conflict handling).
	TruncateSuffix(from uint64) error
	// TruncatePrefix removes all entries with index <= upto (post-snapshot GC).
	TruncatePrefix(upto uint64) error
	// Entries returns log entries in [lo, hi]. Missing entries are an error.
	Entries(lo, hi uint64) ([]LogEntry, error)
	// FirstIndex / LastIndex describe the log bounds of the persistent store.
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)

	// SaveSnapshot persists a snapshot and its metadata atomically.
	SaveSnapshot(lastIndex, lastTerm uint64, data []byte) error
	// LoadSnapshot returns the stored snapshot, or (0,0,nil,nil) if none.
	LoadSnapshot() (lastIndex, lastTerm uint64, data []byte, err error)

	// Close releases resources.
	Close() error
}
