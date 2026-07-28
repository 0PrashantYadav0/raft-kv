package raft

import (
	"context"
	"sort"
	"time"
)

// snapshotRPCTimeout bounds a single InstallSnapshot call. Snapshots can be
// large, so this is generous relative to AppendEntries.
const snapshotRPCTimeout = 30 * time.Second

// Propose submits a command to be replicated. It blocks until the command is
// committed and applied (returning the FSM result) or the context is cancelled.
// Only the leader may propose; otherwise ErrNotLeader is returned so the caller
// can redirect to the current leader.
func (n *Node) Propose(ctx context.Context, cmd []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrShutdown
	}
	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		return nil, ErrNotLeader{Leader: leader}
	}

	index := n.log.lastIndex() + 1
	entry := LogEntry{Term: n.currentTerm, Index: index, Command: cmd}
	n.log.append(entry)
	if err := n.cfg.Storage.AppendLog([]LogEntry{entry}); err != nil {
		n.log.truncateFrom(index)
		n.mu.Unlock()
		return nil, err
	}

	p := &pending{term: n.currentTerm, result: make(chan proposeResult, 1)}
	n.pendings[index] = p
	n.triggerReplication()
	// Self-commit path for a single-node cluster (no peers to replicate to).
	n.maybeAdvanceCommit()
	n.mu.Unlock()

	select {
	case res := <-p.result:
		return res.value, res.err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pendings, index)
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

// triggerReplication wakes every peer replicator. Caller holds n.mu.
func (n *Node) triggerReplication() {
	for _, ch := range n.replicateCh {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// runReplicator drives replication to a single peer: heartbeats on a timer and
// immediately on a trigger, catching the peer up as fast as it can accept.
func (n *Node) runReplicator(ctx context.Context, peer NodeID, wake chan struct{}) {
	defer n.wg.Done()

	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	// Send an immediate heartbeat to assert leadership.
	n.replicateToPeer(ctx, peer)

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.replicateToPeer(ctx, peer)
		case <-wake:
			n.replicateToPeer(ctx, peer)
		}
	}
}

// replicateToPeer sends AppendEntries (or InstallSnapshot) to peer and processes
// the reply, looping until the peer is caught up to lastIndex.
func (n *Node) replicateToPeer(ctx context.Context, peer NodeID) {
	for {
		n.mu.Lock()
		if n.role != Leader || n.stopped {
			n.mu.Unlock()
			return
		}
		next := n.nextIndex[peer]

		// If the peer needs entries we've already compacted, ship a snapshot.
		if next <= n.log.snapshotIndex {
			args := &InstallSnapshotArgs{
				Term:              n.currentTerm,
				LeaderID:          n.id,
				LastIncludedIndex: n.log.snapshotIndex,
				LastIncludedTerm:  n.log.snapshotTerm,
				Done:              true,
			}
			_, _, data, err := n.cfg.Storage.LoadSnapshot()
			if err != nil {
				n.mu.Unlock()
				n.logger.Error("load snapshot for replication", "err", err)
				return
			}
			args.Data = data
			term := n.currentTerm
			n.mu.Unlock()

			rpcCtx, cancel := context.WithTimeout(ctx, snapshotRPCTimeout)
			reply, err := n.cfg.Transport.InstallSnapshot(rpcCtx, peer, args)
			cancel()
			if err != nil {
				return
			}
			n.mu.Lock()
			if n.role != Leader || n.currentTerm != term {
				n.mu.Unlock()
				return
			}
			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				n.mu.Unlock()
				return
			}
			n.lastContact[peer] = time.Now()
			n.matchIndex[peer] = args.LastIncludedIndex
			n.nextIndex[peer] = args.LastIncludedIndex + 1
			n.mu.Unlock()
			continue
		}

		prevIndex := next - 1
		prevTerm, ok := n.log.termAt(prevIndex)
		if !ok {
			// prevIndex was compacted; retry via snapshot path next iteration.
			n.nextIndex[peer] = n.log.snapshotIndex
			n.mu.Unlock()
			continue
		}
		entries := n.log.sliceFrom(next)
		if len(entries) > n.cfg.MaxEntriesPerAppend {
			entries = entries[:n.cfg.MaxEntriesPerAppend]
		}
		args := &AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}
		term := n.currentTerm
		n.mu.Unlock()

		rpcCtx, cancel := context.WithTimeout(ctx, n.cfg.ElectionTimeoutMax)
		reply, err := n.cfg.Transport.AppendEntries(rpcCtx, peer, args)
		cancel()
		if err != nil {
			return // network error; heartbeat will retry
		}

		n.mu.Lock()
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		if reply.Term > n.currentTerm {
			n.stepDown(reply.Term)
			n.mu.Unlock()
			return
		}
		n.lastContact[peer] = time.Now()

		if reply.Success {
			newMatch := prevIndex + uint64(len(entries))
			if newMatch > n.matchIndex[peer] {
				n.matchIndex[peer] = newMatch
			}
			n.nextIndex[peer] = n.matchIndex[peer] + 1
			n.maybeAdvanceCommit()
			caughtUp := n.nextIndex[peer] > n.log.lastIndex()
			n.mu.Unlock()
			if caughtUp {
				return
			}
			continue // more to send
		}

		// Failure: back up nextIndex using the fast-backup hints (§5.3).
		n.nextIndex[peer] = n.backupNextIndex(reply, next)
		n.mu.Unlock()
	}
}

// backupNextIndex computes the new nextIndex after an AppendEntries rejection.
// Caller holds n.mu.
func (n *Node) backupNextIndex(reply *AppendEntriesReply, current uint64) uint64 {
	if reply.ConflictTerm == 0 {
		// Follower's log is too short; jump to its reported end.
		if reply.ConflictIndex == 0 {
			return 1
		}
		return reply.ConflictIndex
	}
	// Look for the last entry we have in ConflictTerm.
	for idx := n.log.lastIndex(); idx >= n.log.firstIndex(); idx-- {
		t, ok := n.log.termAt(idx)
		if !ok {
			break
		}
		if t == reply.ConflictTerm {
			return idx + 1
		}
		if idx == n.log.firstIndex() {
			break
		}
	}
	if reply.ConflictIndex == 0 {
		return 1
	}
	return reply.ConflictIndex
}

// maybeAdvanceCommit advances commitIndex to the highest index replicated on a
// majority, restricted to entries from the current term (§5.4.2). Caller holds
// n.mu.
func (n *Node) maybeAdvanceCommit() {
	// Collect match indexes (self is fully caught up).
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.log.lastIndex())
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	majorityIdx := len(matches) / 2 // 0-based index of the majority element
	candidate := matches[majorityIdx]

	if candidate <= n.commitIndex {
		return
	}
	if term, ok := n.log.termAt(candidate); ok && term == n.currentTerm {
		n.commitIndex = candidate
		n.signalApply()
	}
}

// signalApply nudges the apply loop. Caller holds n.mu.
func (n *Node) signalApply() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

// runApplyLoop applies committed-but-unapplied entries to the FSM in order,
// delivers results to waiting proposers, and triggers snapshots.
func (n *Node) runApplyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applySignal:
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		next := n.lastApplied + 1
		entry, ok := n.log.entryAt(next)
		if !ok {
			// Entry was compacted (snapshot installed); skip ahead.
			n.lastApplied = n.log.snapshotIndex
			n.mu.Unlock()
			continue
		}
		p := n.pendings[next]
		delete(n.pendings, next)
		n.mu.Unlock()

		var result any
		if len(entry.Command) > 0 { // skip no-op entries
			result = n.cfg.FSM.Apply(entry.Command)
		}

		n.mu.Lock()
		n.lastApplied = next
		// Only satisfy the waiter if the entry still belongs to its proposer's
		// term (guards against a committed-then-overwritten edge case).
		if p != nil && p.term == entry.Term {
			p.result <- proposeResult{value: result}
		} else if p != nil {
			p.result <- proposeResult{err: ErrNotLeader{Leader: n.leaderID}}
		}
		n.mu.Unlock()

		n.maybeSnapshot()
	}
}
