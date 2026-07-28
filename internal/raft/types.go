// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014).
//
// The implementation covers the three pillars required for a production-shaped
// system: leader election, log replication, and snapshotting (log compaction
// via InstallSnapshot). The algorithm core is transport-agnostic: it talks to
// peers through the Transport interface, which makes it trivial to unit-test
// with an in-memory transport and to run in production over the network.
package raft

// NodeID is the stable identity of a cluster member.
type NodeID string

// Role is the current role of a node in the Raft state machine.
type Role int

const (
	// Follower is the passive role: it only responds to leaders and candidates.
	Follower Role = iota
	// Candidate is the role a node assumes when it starts an election.
	Candidate
	// Leader is the single node responsible for serving client writes and
	// replicating them to the rest of the cluster.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single record in the replicated log. Term is used to detect
// inconsistencies between logs on different servers, Index is the entry's
// position (1-based), and Command is an opaque payload handed to the FSM once
// the entry is committed.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// RequestVoteArgs are the arguments for the RequestVote RPC (§5.2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	// PreVote marks a disruption-avoiding pre-vote round (§9.6 / Ongaro thesis).
	PreVote bool
}

// RequestVoteReply is the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs are the arguments for the AppendEntries RPC (§5.3). An empty
// Entries slice is a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the reply for the AppendEntries RPC. The Conflict*
// fields implement the fast-backup optimization from §5.3 so a leader can
// resynchronize a lagging follower without decrementing nextIndex one step at
// a time.
type AppendEntriesReply struct {
	Term    uint64
	Success bool

	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs are the arguments for the InstallSnapshot RPC (§7). This
// implementation ships the snapshot in a single message for simplicity; the
// wire types leave room for chunking via Offset/Done.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            uint64
	Data              []byte
	Done              bool
}

// InstallSnapshotReply is the reply for the InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64
}
