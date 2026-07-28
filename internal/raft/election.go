package raft

import (
	"context"
	"sync"
	"time"
)

// runElectionTimer periodically checks whether the election timeout has elapsed
// without hearing from a leader (or granting a vote). If so, it starts an
// election. Leaders do not run elections.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timeout := n.pickTimeout()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role == Leader {
				n.mu.Unlock()
				continue
			}
			if time.Since(n.electionResetAt) >= timeout {
				n.mu.Unlock()
				n.startElection()
				timeout = n.pickTimeout()
				continue
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) pickTimeout() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.randomElectionTimeout()
}

// startElection runs an optional pre-vote round, then a real election. Pre-vote
// prevents a node isolated by a partition from repeatedly incrementing its term
// and forcing needless leader changes when it rejoins.
func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	prospectiveTerm := n.currentTerm + 1
	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	n.electionResetAt = time.Now()
	n.mu.Unlock()

	// Pre-vote: only escalate to a real election if we could win.
	if !n.gatherVotes(prospectiveTerm, lastIdx, lastTerm, true) {
		n.logger.Debug("pre-vote failed; staying follower", "term", prospectiveTerm)
		return
	}

	n.mu.Lock()
	// Re-validate: we may have stepped down or moved on while pre-voting.
	if n.role == Leader || n.stopped || n.currentTerm >= prospectiveTerm {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.currentTerm = prospectiveTerm
	n.votedFor = n.id
	n.leaderID = ""
	n.persistState()
	n.electionResetAt = time.Now()
	term := n.currentTerm
	n.mu.Unlock()

	n.logger.Info("starting election", "term", term)
	if n.gatherVotes(term, lastIdx, lastTerm, false) {
		n.mu.Lock()
		if n.role == Candidate && n.currentTerm == term {
			n.becomeLeader()
		}
		n.mu.Unlock()
	}
}

// gatherVotes fans out RequestVote (or pre-vote) RPCs and reports whether a
// majority (including self) was obtained.
func (n *Node) gatherVotes(term, lastIdx, lastTerm uint64, preVote bool) bool {
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
		PreVote:      preVote,
	}

	var (
		mu        sync.Mutex
		votes     = 1 // vote for self
		responded = 1
		cond      = sync.NewCond(&mu)
	)
	majority := (len(n.peers)+1)/2 + 1

	if majority == 1 { // single-node cluster
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeoutMax)
	defer cancel()

	for _, peer := range n.peers {
		go func(peer NodeID) {
			reply, err := n.cfg.Transport.RequestVote(ctx, peer, args)
			mu.Lock()
			defer mu.Unlock()
			responded++
			if err == nil && reply != nil {
				if reply.Term > term {
					n.mu.Lock()
					if reply.Term > n.currentTerm {
						n.stepDown(reply.Term)
					}
					n.mu.Unlock()
				} else if reply.VoteGranted {
					votes++
				}
			}
			cond.Broadcast()
		}(peer)
	}

	mu.Lock()
	defer mu.Unlock()
	// Watchdog: unblock the waiter if the overall deadline elapses even when a
	// transport ignores context cancellation.
	go func() {
		<-ctx.Done()
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()
	for votes < majority && responded < len(n.peers)+1 && ctx.Err() == nil {
		cond.Wait()
	}
	return votes >= majority
}

// becomeLeader transitions to the Leader role and starts replication. Caller
// holds n.mu.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	n.logger.Info("became leader", "term", n.currentTerm)

	lastIdx := n.log.lastIndex()
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	n.lastContact = make(map[NodeID]time.Time, len(n.peers))
	n.replicateCh = make(map[NodeID]chan struct{}, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
		n.replicateCh[p] = make(chan struct{}, 1)
	}

	n.leaderCtx, n.leaderCancel = context.WithCancel(context.Background())

	// Commit a no-op entry in the new term so that entries from previous terms
	// can be committed safely (§5.4.2). The FSM treats an empty command as a
	// no-op.
	noop := LogEntry{Term: n.currentTerm, Index: lastIdx + 1}
	if err := n.cfg.Storage.AppendLog([]LogEntry{noop}); err != nil {
		n.logger.Error("persist no-op", "err", err)
	} else {
		n.log.append(noop)
	}

	// Advance commit immediately so a single-node cluster (no peers to
	// replicate to) can still commit and apply. For multi-node clusters this is
	// a harmless no-op until a majority acks.
	n.maybeAdvanceCommit()

	for _, p := range n.peers {
		n.wg.Add(1)
		go n.runReplicator(n.leaderCtx, p, n.replicateCh[p])
	}
}
