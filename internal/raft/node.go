package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// ErrNotLeader is returned by Propose when the node is not the leader. Leader
// carries the last known leader (may be empty) so clients can redirect.
type ErrNotLeader struct {
	Leader NodeID
}

func (e ErrNotLeader) Error() string {
	if e.Leader == "" {
		return "raft: not leader (leader unknown)"
	}
	return fmt.Sprintf("raft: not leader (leader is %s)", e.Leader)
}

// ErrShutdown is returned once the node has been stopped.
var ErrShutdown = errors.New("raft: node is shut down")

// Config holds the parameters needed to construct a Node. All durations have
// sane defaults applied in NewNode if left zero.
type Config struct {
	ID        NodeID
	Peers     []NodeID // every member, including this node
	Transport Transport
	Storage   Storage
	FSM       FSM
	Logger    *slog.Logger

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// SnapshotThreshold: take a snapshot once this many entries have
	// accumulated past the last snapshot. Zero disables auto-snapshotting.
	SnapshotThreshold uint64
	// MaxEntriesPerAppend caps the entries shipped in a single AppendEntries.
	MaxEntriesPerAppend int
}

// validate checks the required, user-supplied fields before defaults are
// applied.
func (c *Config) validate() error {
	if c.ID == "" {
		return errors.New("raft: Config.ID is required")
	}
	if c.Transport == nil || c.Storage == nil || c.FSM == nil {
		return errors.New("raft: Transport, Storage and FSM are required")
	}
	if len(c.Peers) == 0 {
		return errors.New("raft: Config.Peers must be non-empty and include this node")
	}
	seen := make(map[NodeID]bool, len(c.Peers))
	includesSelf := false
	for _, p := range c.Peers {
		if p == "" {
			return errors.New("raft: Config.Peers contains an empty id")
		}
		if seen[p] {
			return fmt.Errorf("raft: duplicate peer id %q", p)
		}
		seen[p] = true
		if p == c.ID {
			includesSelf = true
		}
	}
	if !includesSelf {
		return fmt.Errorf("raft: Config.Peers must include this node's id %q", c.ID)
	}
	return nil
}

// validateTimings checks timeout invariants after defaults are applied.
func (c *Config) validateTimings() error {
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return fmt.Errorf("raft: ElectionTimeoutMin (%s) must be < ElectionTimeoutMax (%s)",
			c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	}
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: HeartbeatInterval (%s) must be < ElectionTimeoutMin (%s)",
			c.HeartbeatInterval, c.ElectionTimeoutMin)
	}
	return nil
}

func (c *Config) withDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = 256
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// pending tracks a client Propose awaiting commit+apply.
type pending struct {
	term   uint64
	result chan proposeResult
}

type proposeResult struct {
	value any
	err   error
}

// Node is a single Raft server. It is safe for concurrent use.
type Node struct {
	cfg    Config
	id     NodeID
	peers  []NodeID // peers excluding self
	logger *slog.Logger

	mu sync.Mutex

	// Persistent state (durable before responding to RPCs).
	currentTerm uint64
	votedFor    NodeID
	log         *raftLog

	// Volatile state.
	role        Role
	leaderID    NodeID
	commitIndex uint64
	lastApplied uint64

	// Leader volatile state.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	// lastContact records the last time each peer acknowledged us as leader.
	// It backs the leader lease used for linearizable reads.
	lastContact map[NodeID]time.Time

	// Election bookkeeping.
	electionResetAt time.Time

	// Per-leadership coordination.
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
	replicateCh  map[NodeID]chan struct{} // wake a peer replicator

	// Apply pipeline.
	pendings    map[uint64]*pending
	applySignal chan struct{}

	rng *rand.Rand

	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

// NewNode constructs a Node, restoring persistent state from Storage.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.withDefaults()
	if err := cfg.validateTimings(); err != nil {
		return nil, err
	}

	peers := make([]NodeID, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			peers = append(peers, p)
		}
	}

	n := &Node{
		cfg:         cfg,
		id:          cfg.ID,
		peers:       peers,
		logger:      cfg.Logger.With("node", string(cfg.ID)),
		log:         newRaftLog(),
		role:        Follower,
		nextIndex:   make(map[NodeID]uint64),
		matchIndex:  make(map[NodeID]uint64),
		lastContact: make(map[NodeID]time.Time),
		replicateCh: make(map[NodeID]chan struct{}),
		pendings:    make(map[uint64]*pending),
		applySignal: make(chan struct{}, 1),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(hashID(cfg.ID)))),
		stopCh:      make(chan struct{}),
	}

	if err := n.restore(); err != nil {
		return nil, err
	}
	return n, nil
}

func hashID(id NodeID) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return h
}

// restore loads persistent term/vote, the snapshot, and the durable log.
func (n *Node) restore() error {
	term, votedFor, err := n.cfg.Storage.LoadState()
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	n.currentTerm = term
	n.votedFor = votedFor

	snapIndex, snapTerm, snapData, err := n.cfg.Storage.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	if snapIndex > 0 {
		if err := n.cfg.FSM.Restore(snapData); err != nil {
			return fmt.Errorf("restore fsm: %w", err)
		}
		n.log.snapshotIndex = snapIndex
		n.log.snapshotTerm = snapTerm
		n.commitIndex = snapIndex
		n.lastApplied = snapIndex
	}

	first, err := n.cfg.Storage.FirstIndex()
	if err != nil {
		return fmt.Errorf("first index: %w", err)
	}
	last, err := n.cfg.Storage.LastIndex()
	if err != nil {
		return fmt.Errorf("last index: %w", err)
	}
	if last >= first && first > 0 {
		entries, err := n.cfg.Storage.Entries(first, last)
		if err != nil {
			return fmt.Errorf("load entries: %w", err)
		}
		// Validate contiguity: the raftLog invariant requires no gaps.
		for i, e := range entries {
			want := first + uint64(i)
			if e.Index != want {
				return fmt.Errorf("corrupt log: expected index %d at position %d, got %d", want, i, e.Index)
			}
		}
		n.log.append(entries...)
	}
	n.logger.Info("restored raft state",
		"term", n.currentTerm, "snapshotIndex", snapIndex,
		"lastIndex", n.log.lastIndex())
	return nil
}

// Start launches the node's background goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	n.electionResetAt = time.Now()
	n.mu.Unlock()

	n.wg.Add(2)
	go n.runElectionTimer()
	go n.runApplyLoop()
	n.logger.Info("node started", "peers", n.peers)
}

// Stop halts all background activity and closes storage.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	if n.leaderCancel != nil {
		n.leaderCancel()
	}
	close(n.stopCh)
	// Fail any in-flight proposals.
	for idx, p := range n.pendings {
		p.result <- proposeResult{err: ErrShutdown}
		delete(n.pendings, idx)
	}
	n.mu.Unlock()

	n.wg.Wait()
	_ = n.cfg.Storage.Close()
	n.logger.Info("node stopped")
}

// persistState durably writes currentTerm+votedFor. Caller holds n.mu. Returns
// an error so callers on the safety-critical path (granting a vote) can refuse
// to proceed if the vote can't be made durable.
func (n *Node) persistState() error {
	if err := n.cfg.Storage.SaveState(n.currentTerm, n.votedFor); err != nil {
		n.logger.Error("failed to persist state", "err", err)
		return err
	}
	return nil
}

// State returns a snapshot of the node's role, term and known leader.
func (n *Node) State() (role Role, term uint64, leader NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm, n.leaderID
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// CanServeRead reports whether this node may serve a linearizable read now. It
// must (1) be the leader, (2) have committed an entry in its current term (so
// its commit index reflects the latest state), and (3) hold a quorum lease — a
// majority of peers acknowledged it as leader within one election timeout. This
// prevents a partitioned old leader from serving stale reads. (A full ReadIndex
// implementation would additionally confirm with a fresh heartbeat round; the
// lease approach assumes bounded clock drift.)
func (n *Node) CanServeRead() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return false
	}
	if t, ok := n.log.termAt(n.commitIndex); !ok || t != n.currentTerm {
		return false
	}
	votes := 1 // self
	cutoff := time.Now().Add(-n.cfg.ElectionTimeoutMin)
	for _, p := range n.peers {
		if ts, ok := n.lastContact[p]; ok && ts.After(cutoff) {
			votes++
		}
	}
	majority := (len(n.peers)+1)/2 + 1
	return votes >= majority
}

// randomElectionTimeout returns a randomized timeout in [min, max). Caller may
// hold n.mu (uses n.rng which is only touched under lock).
func (n *Node) randomElectionTimeout() time.Duration {
	min := n.cfg.ElectionTimeoutMin
	max := n.cfg.ElectionTimeoutMax
	return min + time.Duration(n.rng.Int63n(int64(max-min)))
}

// stepDown converts the node to a follower at the given (higher-or-equal) term.
// Caller holds n.mu.
func (n *Node) stepDown(term uint64) {
	wasLeader := n.role == Leader
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistState()
	}
	n.role = Follower
	if wasLeader && n.leaderCancel != nil {
		n.leaderCancel()
		n.leaderCancel = nil
		n.leaderCtx = nil
	}
}

// --- RPC handlers (RPCHandler interface) ---

// HandleRequestVote implements the RequestVote RPC receiver (§5.2, §5.4).
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	if args.Term < n.currentTerm {
		return reply
	}
	// Leader stickiness: refuse pre-votes while we're still hearing from a
	// healthy leader, so a node isolated by a partition can't force needless
	// elections when it rejoins (Ongaro thesis §4.2.3 / §9.6).
	if args.PreVote && n.leaderID != "" && time.Since(n.electionResetAt) < n.cfg.ElectionTimeoutMin {
		return reply
	}
	// A pre-vote does not mutate our term or vote; it only checks whether we
	// *would* grant a vote, which prevents partitioned nodes from disrupting a
	// stable leader by bumping terms (Ongaro thesis §9.6).
	if !args.PreVote && args.Term > n.currentTerm {
		n.stepDown(args.Term)
		reply.Term = n.currentTerm
	}

	upToDate := n.candidateLogUpToDate(args.LastLogIndex, args.LastLogTerm)
	effectiveTerm := n.currentTerm
	if args.PreVote {
		effectiveTerm = args.Term // evaluate as if the election had happened
	}

	canVote := args.Term >= effectiveTerm &&
		(args.PreVote || n.votedFor == "" || n.votedFor == args.CandidateID)

	if canVote && upToDate {
		reply.VoteGranted = true
		if !args.PreVote {
			// Only grant a real vote once it is durably recorded; otherwise a
			// crash could let us vote again in the same term (§5.2 safety).
			prev := n.votedFor
			n.votedFor = args.CandidateID
			if err := n.persistState(); err != nil {
				n.votedFor = prev
				reply.VoteGranted = false
			} else {
				n.electionResetAt = time.Now()
			}
		}
	}
	return reply
}

// candidateLogUpToDate implements the log-completeness check of §5.4.1.
func (n *Node) candidateLogUpToDate(lastIdx, lastTerm uint64) bool {
	myIdx := n.log.lastIndex()
	myTerm := n.log.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= myIdx
}

// HandleAppendEntries implements the AppendEntries RPC receiver (§5.3).
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm, Success: false}
	if args.Term < n.currentTerm {
		return reply
	}
	// Valid leader for this term: (re)set to follower and reset election timer.
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	} else if n.role != Follower {
		n.stepDown(args.Term)
	}
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()
	reply.Term = n.currentTerm

	// If PrevLogIndex is below our snapshot boundary the leader is referencing
	// entries we've already committed and compacted. Reject (rather than ack)
	// so the leader backs its nextIndex up to snapshotIndex+1 and retries with a
	// PrevLogIndex we can actually verify; acking here would let the leader
	// inflate matchIndex for entries we never stored (a lost-write window).
	if args.PrevLogIndex < n.log.snapshotIndex {
		reply.Success = false
		reply.ConflictIndex = n.log.snapshotIndex + 1
		return reply
	}

	// Consistency check: does our log contain PrevLogIndex/PrevLogTerm?
	if args.PrevLogIndex > n.log.lastIndex() {
		reply.ConflictIndex = n.log.lastIndex() + 1
		reply.ConflictTerm = 0
		return reply
	}
	term, ok := n.log.termAt(args.PrevLogIndex)
	if !ok {
		// Should not happen given the guard above, but stay safe.
		reply.ConflictIndex = n.log.snapshotIndex + 1
		return reply
	}
	if term != args.PrevLogTerm {
		// Fast backup: report the first index of the conflicting term.
		reply.ConflictTerm = term
		reply.ConflictIndex = n.firstIndexOfTerm(term)
		return reply
	}

	// Append any new entries, deleting conflicting suffix (§5.3 rule 3 & 4).
	// Persistence must succeed before we acknowledge, otherwise a positive
	// reply would let the leader commit an entry we might lose on crash.
	if err := n.mergeEntries(args.PrevLogIndex, args.Entries); err != nil {
		n.logger.Error("merge entries failed; rejecting append", "err", err)
		reply.Success = false
		return reply
	}

	// Advance commit index.
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(args.LeaderCommit, n.log.lastIndex())
		n.signalApply()
	}

	reply.Success = true
	return reply
}

// mergeEntries appends new entries after prevLogIndex, truncating on conflict.
// It persists any appended entries durably (in a single batch) before
// returning; a storage error is propagated so the caller can refuse to ack.
// Caller holds n.mu.
func (n *Node) mergeEntries(prevLogIndex uint64, entries []LogEntry) error {
	toAppend := make([]LogEntry, 0, len(entries))
	truncateAt := uint64(0)

	for i, e := range entries {
		idx := prevLogIndex + 1 + uint64(i)
		// Skip entries already covered by our snapshot/committed prefix.
		if idx <= n.log.snapshotIndex {
			continue
		}
		if idx <= n.log.lastIndex() {
			if existing, ok := n.log.termAt(idx); ok && existing == e.Term {
				continue // already have a matching entry
			}
			// Conflict: mark for truncation from here.
			if truncateAt == 0 || idx < truncateAt {
				truncateAt = idx
			}
		}
		toAppend = append(toAppend, e)
	}

	// Apply truncation durably first so the persistent log matches memory.
	if truncateAt != 0 {
		if err := n.cfg.Storage.TruncateSuffix(truncateAt); err != nil {
			return fmt.Errorf("truncate suffix at %d: %w", truncateAt, err)
		}
		n.log.truncateFrom(truncateAt)
	}

	if len(toAppend) == 0 {
		return nil
	}
	// Persist the whole batch in one transaction (one fsync) before publishing
	// to the in-memory log.
	if err := n.cfg.Storage.AppendLog(toAppend); err != nil {
		return fmt.Errorf("persist %d entries: %w", len(toAppend), err)
	}
	n.log.append(toAppend...)
	return nil
}

// firstIndexOfTerm returns the first (lowest) log index whose entry has the
// given term, scanning upward. Caller holds n.mu.
func (n *Node) firstIndexOfTerm(term uint64) uint64 {
	for idx := n.log.firstIndex(); idx <= n.log.lastIndex(); idx++ {
		if t, ok := n.log.termAt(idx); ok && t == term {
			return idx
		}
	}
	return n.log.firstIndex()
}

// HandleInstallSnapshot implements the InstallSnapshot RPC receiver (§7).
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()
	reply.Term = n.currentTerm

	// Ignore snapshots we already subsume. Guard on lastApplied (not just the
	// snapshot boundary) so we never regress already-applied FSM state.
	if args.LastIncludedIndex <= n.lastApplied || args.LastIncludedIndex <= n.log.snapshotIndex {
		return reply
	}

	// §7 retain rule: if we already hold an entry at LastIncludedIndex whose
	// term matches, the entries after it are consistent and can be kept;
	// otherwise the whole log suffix is divergent and must be discarded.
	keepSuffix := false
	if term, ok := n.log.termAt(args.LastIncludedIndex); ok && term == args.LastIncludedTerm {
		keepSuffix = true
	}

	// Persist the snapshot durably BEFORE mutating volatile FSM/log state, so a
	// crash mid-install can't leave the FSM ahead of anything durable.
	if err := n.cfg.Storage.SaveSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
		n.logger.Error("persist installed snapshot", "err", err)
		return reply
	}
	if err := n.cfg.FSM.Restore(args.Data); err != nil {
		n.logger.Error("restore snapshot into fsm", "err", err)
		return reply
	}

	if keepSuffix {
		n.log.compact(args.LastIncludedIndex, args.LastIncludedTerm)
		if err := n.cfg.Storage.TruncatePrefix(args.LastIncludedIndex); err != nil {
			n.logger.Error("truncate prefix after snapshot", "err", err)
		}
	} else {
		n.log.resetToSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)
		// Wipe the entire persistent log (prefix + any divergent suffix).
		if err := n.cfg.Storage.TruncatePrefix(args.LastIncludedIndex); err != nil {
			n.logger.Error("truncate prefix after snapshot", "err", err)
		}
		if err := n.cfg.Storage.TruncateSuffix(args.LastIncludedIndex + 1); err != nil {
			n.logger.Error("truncate suffix after snapshot", "err", err)
		}
	}

	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	n.lastApplied = args.LastIncludedIndex
	// If a committed suffix was retained, make sure it gets applied promptly
	// rather than waiting for the next heartbeat.
	if n.commitIndex > n.lastApplied {
		n.signalApply()
	}
	n.logger.Info("installed snapshot", "lastIncludedIndex", args.LastIncludedIndex)
	return reply
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
