// Package tail incrementally follows local log files.
//
// It never re-reads a file from the beginning on each poll: only bytes past
// the last read offset are returned. File rotation (inode change) and
// truncation (same inode, size moved backwards) are detected and reported as
// explicit events on a new generation, so a rotated-in file can never be
// mistaken for the tail of the old one.
package tail

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"syscall"
)

const maxFingerprint = 256

// Identity pins a specific file incarnation plus the digest of a verified
// prefix of it.
type Identity struct {
	Dev   uint64
	Inode uint64
	// HeadFingerprint digests the first HeadLen bytes (at most 256).
	HeadFingerprint string
	HeadLen         int64
}

// sameIncarnation reports device/inode equality.
func (i Identity) sameIncarnation(o Identity) bool {
	return i.Dev == o.Dev && i.Inode == o.Inode
}

// prefixStillPresent reports that the prefix i digested is still byte-for-
// byte present at the start of a file with current identity cur. When i was
// sampled from an empty file there is no prefix to check.
func (i Identity) prefixStillPresent(path string, size int64) bool {
	if i.HeadFingerprint == "" || i.HeadLen == 0 {
		return true
	}
	if size < i.HeadLen {
		return false
	}
	now, err := identityOf(path, i.HeadLen)
	if err != nil {
		return false
	}
	return now.HeadFingerprint == i.HeadFingerprint
}

// EventKind enumerates tailer events.
type EventKind int

const (
	EventData EventKind = iota
	EventRotation
	EventTruncation
)

// Event is one boundary notification (rotation/truncation/appearance).
type Event struct {
	Kind     EventKind
	Gen      uint64
	Identity Identity
}

// Block is a chunk of appended bytes with exact generation-local offsets.
type Block struct {
	Gen      uint64
	Identity Identity
	Data     []byte
	Start    int64
	End      int64
}

// ReadChunk bounds a single incremental read.
type ReadChunk struct{ MaxBytes int }

// Tailer follows one path, reopening across rotation/truncation.
type Tailer struct {
	path string
	max  int

	f      *os.File
	id     Identity
	offset int64
	gen    uint64
	known  bool

	// Rotation gate: bytes drained from the old inode are delivered before
	// the boundary event; only afterwards is the new file readable.
	drained    []Block
	boundary   *Event
	nextF      *os.File
	nextID     Identity
	prevOffset int64
	prevGen    uint64
}

// Open creates a Tailer. Reading resumes at resumeOffset when the same
// incarnation still contains both that offset and the originally verified
// prefix; otherwise the current file is read from its beginning.
func Open(path string, resumeOffset int64, resumeID Identity, hasResume bool) (*Tailer, error) {
	t := &Tailer{path: path, max: 64 << 10}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return t, nil // appear on a later poll
		}
		return nil, err
	}
	size := info.Size()
	cur, err := identityOf(path, size)
	if err != nil {
		return nil, err
	}
	offset := int64(0)
	if hasResume && resumeID.sameIncarnation(cur) &&
		resumeOffset >= 0 && resumeOffset <= size &&
		resumeID.prefixStillPresent(path, size) {
		offset = resumeOffset
		cur = resumeID // keep the verified prefix as this generation baseline
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	t.f, t.id, t.offset, t.known, t.gen = f, cur, offset, true, 1
	return t, nil
}

func (t *Tailer) Close() error {
	if t.nextF != nil {
		_ = t.nextF.Close()
	}
	if t.f != nil {
		return t.f.Close()
	}
	return nil
}

func (t *Tailer) Gen() uint64        { return t.gen }
func (t *Tailer) PrevGen() uint64    { return t.prevGen }
func (t *Tailer) Offset() int64      { return t.offset }
func (t *Tailer) OldOffset() int64   { return t.prevOffset }
func (t *Tailer) Identity() Identity { return t.id }
func (t *Tailer) Path() string       { return t.path }

// Poll checks the path for appearance, rotation and truncation. After a
// rotation the old inode is fully drained first: drained Blocks come out of
// Read, and only when they are exhausted does Poll return the boundary.
func (t *Tailer) Poll() (*Event, error) {
	info, err := os.Stat(t.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	size := info.Size()

	if !t.known {
		cur, err := identityOf(t.path, size)
		if err != nil {
			return nil, err
		}
		f, err := os.Open(t.path)
		if err != nil {
			return nil, err
		}
		t.f, t.id, t.offset, t.known, t.gen = f, cur, 0, true, 1
		return &Event{Kind: EventData, Gen: t.gen, Identity: cur}, nil
	}

	cur, err := identityStat(info)
	if err != nil {
		return nil, err
	}
	switch {
	case cur.sameIncarnation(t.id) && size < t.offset:
		if _, err := t.f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		t.prevOffset, t.prevGen = t.offset, t.gen
		t.offset = 0
		t.gen++
		t.id = cur // same incarnation, truncated: fingerprint recomputed
		return &Event{Kind: EventTruncation, Gen: t.gen, Identity: cur}, nil
	case !cur.sameIncarnation(t.id) || !t.id.prefixStillPresent(t.path, size):
		return t.armRotation(cur)
	}
	return nil, nil
}

func (t *Tailer) armRotation(cur Identity) (*Event, error) {
	if t.boundary == nil {
		oldGen := t.gen
		for {
			b, err := t.readFrom(t.f, oldGen, t.id)
			if err != nil {
				return nil, err
			}
			if b == nil {
				break
			}
			t.drained = append(t.drained, *b)
		}
		nf, err := os.Open(t.path)
		if err != nil {
			return nil, err
		}
		t.nextF, t.nextID = nf, cur
		t.boundary = &Event{Kind: EventRotation, Gen: oldGen + 1, Identity: cur}
	}
	if len(t.drained) > 0 {
		return nil, nil
	}
	if t.f != nil {
		_ = t.f.Close()
	}
	t.f = t.nextF
	t.nextF = nil
	t.prevOffset, t.prevGen = t.offset, t.gen
	t.id, t.offset, t.gen = t.nextID, 0, t.boundary.Gen
	ev := t.boundary
	t.boundary = nil
	return ev, nil
}

// Read returns one incrementally appended chunk, or nil at EOF.
func (t *Tailer) Read() (*Block, error) {
	if len(t.drained) > 0 {
		b := t.drained[0]
		t.drained = t.drained[1:]
		return &b, nil
	}
	if !t.known {
		return nil, nil
	}
	return t.readFrom(t.f, t.gen, t.id)
}

func (t *Tailer) readFrom(f *os.File, gen uint64, id Identity) (*Block, error) {
	max := t.max
	if max <= 0 {
		max = 64 << 10
	}
	buf := make([]byte, max)
	n, err := f.Read(buf)
	if n > 0 {
		start := t.offset
		t.offset += int64(n)
		b := &Block{Gen: gen, Identity: id, Data: buf[:n], Start: start, End: t.offset}
		if errors.Is(err, io.EOF) {
			return b, nil
		}
		return b, err
	}
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return nil, err
}

// identityOf digests the first min(want, size) bytes (capped at 256).
func identityOf(path string, want int64) (Identity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Identity{}, err
	}
	id, err := identityStat(info)
	if err != nil {
		return id, err
	}
	n := info.Size()
	if want > 0 && want < n {
		n = want
	}
	if n > maxFingerprint {
		n = maxFingerprint
	}
	if n > 0 {
		f, err := os.Open(path)
		if err != nil {
			return id, err
		}
		defer f.Close()
		head := make([]byte, n)
		if _, err := io.ReadFull(f, head); err != nil {
			return id, err
		}
		sum := sha256.Sum256(head)
		id.HeadFingerprint = hex.EncodeToString(sum[:])
		id.HeadLen = n
	}
	return id, nil
}

func identityStat(info os.FileInfo) (Identity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return Identity{}, errors.New("unsupported platform: cannot read file identity")
	}
	return Identity{Dev: uint64(stat.Dev), Inode: stat.Ino}, nil
}
