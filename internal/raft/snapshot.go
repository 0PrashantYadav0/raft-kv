package raft

// maybeSnapshot takes a snapshot of the FSM and compacts the log once enough
// entries have accumulated past the previous snapshot. It is only ever called
// from the single apply-loop goroutine, so the FSM state is stable at
// lastApplied for the duration of FSM.Snapshot().
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	if n.cfg.SnapshotThreshold == 0 ||
		n.lastApplied-n.log.snapshotIndex < n.cfg.SnapshotThreshold {
		n.mu.Unlock()
		return
	}
	snapIndex := n.lastApplied
	snapTerm, ok := n.log.termAt(snapIndex)
	if !ok {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	data, err := n.cfg.FSM.Snapshot()
	if err != nil {
		n.logger.Error("fsm snapshot", "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	// A concurrent InstallSnapshot may have advanced us past snapIndex.
	if snapIndex <= n.log.snapshotIndex {
		return
	}
	if err := n.cfg.Storage.SaveSnapshot(snapIndex, snapTerm, data); err != nil {
		n.logger.Error("persist snapshot", "err", err)
		return
	}
	n.log.compact(snapIndex, snapTerm)
	if err := n.cfg.Storage.TruncatePrefix(snapIndex); err != nil {
		n.logger.Error("truncate prefix", "err", err)
	}
	n.logger.Info("took snapshot", "lastIncludedIndex", snapIndex, "bytes", len(data))
}
