package kv

import (
	"bytes"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	s := NewStore()

	cmd, _ := Command{Op: OpPut, Key: "a", Value: []byte("1")}.Encode()
	if prev := s.Apply(cmd); prev != nil {
		if p, ok := prev.([]byte); !ok || p != nil {
			t.Fatalf("expected nil previous value, got %v", prev)
		}
	}
	if v, ok := s.Get("a"); !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("get a = %q ok=%v", v, ok)
	}

	del, _ := Command{Op: OpDelete, Key: "a"}.Encode()
	s.Apply(del)
	if _, ok := s.Get("a"); ok {
		t.Fatal("key a should be deleted")
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := NewStore()
	for _, k := range []string{"x", "y", "z"} {
		cmd, _ := Command{Op: OpPut, Key: k, Value: []byte(k + "!")}.Encode()
		s.Apply(cmd)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restored := NewStore()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.Len() != 3 {
		t.Fatalf("expected 3 keys, got %d", restored.Len())
	}
	if v, ok := restored.Get("y"); !ok || string(v) != "y!" {
		t.Fatalf("restored y = %q ok=%v", v, ok)
	}
}

func TestApplyBadCommandDoesNotPanic(t *testing.T) {
	s := NewStore()
	if got := s.Apply([]byte("garbage")); got == nil {
		t.Fatal("expected an error result for malformed command")
	}
}
