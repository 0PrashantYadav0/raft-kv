package raft

// raftLog is an in-memory view of the replicated log that is snapshot-aware.
// Entries before snapshotIndex have been compacted away; snapshotIndex/Term act
// as the "entry before the first real entry" for the AppendEntries consistency
// check. It is the fast path; the Node mirrors every mutation to durable
// Storage.
//
// Invariant: entries[i].Index == snapshotIndex + 1 + i.
type raftLog struct {
	entries       []LogEntry
	snapshotIndex uint64
	snapshotTerm  uint64
}

func newRaftLog() *raftLog {
	return &raftLog{}
}

// lastIndex is the index of the last entry, or snapshotIndex if the log is empty.
func (l *raftLog) lastIndex() uint64 {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Index
	}
	return l.snapshotIndex
}

// lastTerm is the term of the last entry, or the snapshot term if empty.
func (l *raftLog) lastTerm() uint64 {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Term
	}
	return l.snapshotTerm
}

// firstIndex is the index of the first in-memory entry (snapshotIndex + 1).
func (l *raftLog) firstIndex() uint64 {
	return l.snapshotIndex + 1
}

// termAt returns the term of the entry at index, and whether it is known.
// It knows the snapshot boundary term. Indexes compacted away (except the
// snapshot boundary itself) return (0, false).
func (l *raftLog) termAt(index uint64) (uint64, bool) {
	if index == l.snapshotIndex {
		return l.snapshotTerm, true
	}
	if index < l.firstIndex() || index > l.lastIndex() {
		return 0, false
	}
	return l.entries[index-l.firstIndex()].Term, true
}

// entryAt returns the entry at index and whether it exists in memory.
func (l *raftLog) entryAt(index uint64) (LogEntry, bool) {
	if index < l.firstIndex() || index > l.lastIndex() {
		return LogEntry{}, false
	}
	return l.entries[index-l.firstIndex()], true
}

// sliceFrom returns a copy of all entries with index >= from that are in memory.
func (l *raftLog) sliceFrom(from uint64) []LogEntry {
	if from < l.firstIndex() {
		from = l.firstIndex()
	}
	if from > l.lastIndex() {
		return nil
	}
	src := l.entries[from-l.firstIndex():]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// append adds entries to the end of the log.
func (l *raftLog) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// truncateFrom removes all entries with index >= from. It refuses to touch the
// compacted/committed prefix (index <= snapshotIndex): truncating there would
// silently destroy committed state, so such a call is treated as a no-op. This
// is a defensive guard; callers must never legitimately truncate below the
// snapshot boundary.
func (l *raftLog) truncateFrom(from uint64) {
	if from <= l.snapshotIndex {
		return
	}
	if from > l.lastIndex() {
		return
	}
	l.entries = l.entries[:from-l.firstIndex()]
}

// compact discards entries up to and including lastIndex, recording the
// snapshot boundary. Used after taking or installing a snapshot.
func (l *raftLog) compact(lastIndex, lastTerm uint64) {
	if lastIndex <= l.snapshotIndex {
		return
	}
	// Keep any entries strictly after lastIndex.
	var kept []LogEntry
	if lastIndex < l.lastIndex() {
		kept = l.sliceFrom(lastIndex + 1)
	}
	l.entries = kept
	l.snapshotIndex = lastIndex
	l.snapshotTerm = lastTerm
}

// resetToSnapshot discards the entire in-memory log and sets the snapshot
// boundary. Used when an installed snapshot's boundary term does not match our
// log, so the whole suffix must be discarded (§7).
func (l *raftLog) resetToSnapshot(index, term uint64) {
	l.entries = nil
	l.snapshotIndex = index
	l.snapshotTerm = term
}
