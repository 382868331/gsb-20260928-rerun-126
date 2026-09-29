package tail

import (
	"os"
	"path/filepath"
	"testing"
)

func readAll(t *testing.T, tl *Tailer) []byte {
	t.Helper()
	var out []byte
	for {
		b, err := tl.Read()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return out
		}
		out = append(out, b.Data...)
	}
}

func poll(t *testing.T, tl *Tailer) *Event {
	t.Helper()
	ev, err := tl.Poll()
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func TestIncrementalReadsOnly(t *testing.T) {
	p := filepath.Join(t.TempDir(), "a.log")
	os.WriteFile(p, []byte("AAAAA\n"), 0o644)
	tl, err := Open(p, 0, Identity{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tl.Close()
	if got := readAll(t, tl); string(got) != "AAAAA\n" {
		t.Fatalf("first read = %q", got)
	}
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("BB\n")
	f.Close()
	poll(t, tl)
	if got := readAll(t, tl); string(got) != "BB\n" {
		t.Fatalf("incremental read must return only new bytes, got %q", got)
	}
	poll(t, tl)
	if got := readAll(t, tl); got != nil {
		t.Fatalf("no new data expected, got %q", got)
	}
}

func TestRotationDrainsOldThenReadsNewFromZero(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.log")
	os.WriteFile(p, []byte("old1\n"), 0o644)
	tl, _ := Open(p, 0, Identity{}, false)
	defer tl.Close()
	readAll(t, tl)

	os.Rename(p, p+".1")
	// append to old inode after rename
	f, _ := os.OpenFile(p+".1", os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("old-tail\n")
	f.Close()
	os.WriteFile(p, []byte("new1\n"), 0o644)

	// First polls surface drained old-inode bytes, no boundary yet.
	var drained []byte
	for i := 0; i < 50; i++ {
		ev := poll(t, tl)
		if ev != nil {
			if ev.Kind != EventRotation {
				t.Fatalf("want rotation, got %v", ev.Kind)
			}
			break
		}
		var part []byte
		for {
			b, err := tl.Read()
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			part = append(part, b.Data...)
		}
		drained = append(drained, part...)
	}
	if string(drained) != "old-tail\n" {
		t.Fatalf("old inode must be drained before boundary, got %q", drained)
	}
	if got := readAll(t, tl); string(got) != "new1\n" {
		t.Fatalf("new file read from start, got %q", got)
	}
	if tl.Offset() != int64(len("new1\n")) || tl.Gen() != 2 {
		t.Fatalf("new generation offset/gen wrong: off=%d gen=%d", tl.Offset(), tl.Gen())
	}
}

func TestTruncationEventAndZeroRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "a.log")
	os.WriteFile(p, []byte("0123456789\n"), 0o644)
	tl, _ := Open(p, 0, Identity{}, false)
	defer tl.Close()
	readAll(t, tl)

	os.Truncate(p, 0)
	ev := poll(t, tl)
	if ev == nil || ev.Kind != EventTruncation {
		t.Fatalf("want truncation event, got %+v", ev)
	}
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("x\n")
	f.Close()
	poll(t, tl)
	if got := readAll(t, tl); string(got) != "x\n" {
		t.Fatalf("truncated file must be read from zero, got %q", got)
	}
}

func TestResumeWithinFingerprintWindow(t *testing.T) {
	p := filepath.Join(t.TempDir(), "a.log")
	os.WriteFile(p, []byte("first-line\n"), 0o644)
	tl, _ := Open(p, 0, Identity{}, false)
	readAll(t, tl)
	id, off := tl.Identity(), tl.Offset()
	tl.Close()

	// Grow the file within the fingerprint window; same inode.
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("second\n")
	f.Close()

	tl2, err := Open(p, off, id, true)
	if err != nil {
		t.Fatal(err)
	}
	defer tl2.Close()
	if tl2.Offset() != off {
		t.Fatalf("must resume at %d, got %d (false rotation)", off, tl2.Offset())
	}
	if got := readAll(t, tl2); string(got) != "second\n" {
		t.Fatalf("resume must only yield new bytes, got %q", got)
	}
}

func TestResumeAgainstInodeReuseRestartsAtZero(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.log")
	os.WriteFile(p, []byte("original-content-here\n"), 0o644)
	tl, _ := Open(p, 0, Identity{}, false)
	readAll(t, tl)
	id, off := tl.Identity(), tl.Offset()
	tl.Close()

	// Replace with different content. On most test filesystems the inode may
	// be reused; the prefix digest must detect the mismatch regardless.
	os.Remove(p)
	os.WriteFile(p, []byte("zzzz totally different bytes here\n"), 0o644)
	tl2, err := Open(p, off, id, true)
	if err != nil {
		t.Fatal(err)
	}
	defer tl2.Close()
	if tl2.Offset() != 0 {
		t.Fatalf("foreign content must never resume mid-file, got %d", tl2.Offset())
	}
}
