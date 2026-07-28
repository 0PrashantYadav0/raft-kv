package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/prashant/raft-kv/internal/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketMeta     = []byte("meta")
	bucketLog      = []byte("log")
	bucketSnapshot = []byte("snapshot")

	keyTerm      = []byte("term")
	keyVotedFor  = []byte("votedFor")
	keySnapIndex = []byte("snapIndex")
	keySnapTerm  = []byte("snapTerm")
	keySnapData  = []byte("snapData")
)

// Bolt is a durable raft.Storage backed by bbolt (a single-file B+tree store).
// Every mutating call runs in a write transaction, which fsyncs on commit, so
// state is durable before the method returns.
type Bolt struct {
	db *bolt.DB
}

// OpenBolt opens (or creates) a Bolt store at path.
func OpenBolt(path string) (*Bolt, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketLog, bucketSnapshot} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Bolt{db: db}, nil
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// encodeEntry: [term:8][index:8][command...].
func encodeEntry(e raft.LogEntry) []byte {
	buf := make([]byte, 16+len(e.Command))
	binary.BigEndian.PutUint64(buf[0:8], e.Term)
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	copy(buf[16:], e.Command)
	return buf
}

func decodeEntry(b []byte) raft.LogEntry {
	e := raft.LogEntry{
		Term:  binary.BigEndian.Uint64(b[0:8]),
		Index: binary.BigEndian.Uint64(b[8:16]),
	}
	if len(b) > 16 {
		e.Command = append([]byte(nil), b[16:]...)
	}
	return e
}

func (s *Bolt) SaveState(term uint64, votedFor raft.NodeID) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if err := b.Put(keyTerm, u64(term)); err != nil {
			return err
		}
		return b.Put(keyVotedFor, []byte(votedFor))
	})
}

func (s *Bolt) LoadState() (uint64, raft.NodeID, error) {
	var term uint64
	var votedFor raft.NodeID
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if v := b.Get(keyTerm); v != nil {
			term = binary.BigEndian.Uint64(v)
		}
		if v := b.Get(keyVotedFor); v != nil {
			votedFor = raft.NodeID(v)
		}
		return nil
	})
	return term, votedFor, err
}

func (s *Bolt) AppendLog(entries []raft.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		for _, e := range entries {
			if err := b.Put(u64(e.Index), encodeEntry(e)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Bolt) TruncateSuffix(from uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		for k, _ := c.Seek(u64(from)); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Bolt) TruncatePrefix(upto uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if binary.BigEndian.Uint64(k) > upto {
				break
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Bolt) Entries(lo, hi uint64) ([]raft.LogEntry, error) {
	var out []raft.LogEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		for k, v := c.Seek(u64(lo)); k != nil; k, v = c.Next() {
			idx := binary.BigEndian.Uint64(k)
			if idx > hi {
				break
			}
			out = append(out, decodeEntry(v))
		}
		return nil
	})
	return out, err
}

func (s *Bolt) FirstIndex() (uint64, error) {
	var idx uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		if k, _ := b.Cursor().First(); k != nil {
			idx = binary.BigEndian.Uint64(k)
			return nil
		}
		// Empty log: first index is snapIndex+1.
		if v := tx.Bucket(bucketSnapshot).Get(keySnapIndex); v != nil {
			idx = binary.BigEndian.Uint64(v) + 1
		} else {
			idx = 1
		}
		return nil
	})
	return idx, err
}

func (s *Bolt) LastIndex() (uint64, error) {
	var idx uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		if k, _ := b.Cursor().Last(); k != nil {
			idx = binary.BigEndian.Uint64(k)
			return nil
		}
		if v := tx.Bucket(bucketSnapshot).Get(keySnapIndex); v != nil {
			idx = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return idx, err
}

func (s *Bolt) SaveSnapshot(lastIndex, lastTerm uint64, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSnapshot)
		if err := b.Put(keySnapIndex, u64(lastIndex)); err != nil {
			return err
		}
		if err := b.Put(keySnapTerm, u64(lastTerm)); err != nil {
			return err
		}
		return b.Put(keySnapData, data)
	})
}

func (s *Bolt) LoadSnapshot() (uint64, uint64, []byte, error) {
	var lastIndex, lastTerm uint64
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSnapshot)
		if v := b.Get(keySnapIndex); v != nil {
			lastIndex = binary.BigEndian.Uint64(v)
		}
		if v := b.Get(keySnapTerm); v != nil {
			lastTerm = binary.BigEndian.Uint64(v)
		}
		if v := b.Get(keySnapData); v != nil {
			data = append([]byte(nil), v...)
		}
		return nil
	})
	return lastIndex, lastTerm, data, err
}

func (s *Bolt) Close() error { return s.db.Close() }
