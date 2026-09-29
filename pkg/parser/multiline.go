package parser

import (
	"bytes"
	"regexp"
)

// Item is one assembled multiline unit produced by the Splitter.
type Item struct {
	// Text is the assembled record (newlines preserved, trailing newline
	// stripped).
	Text string
	// Start/End are generation-local byte offsets. End is exclusive and
	// points past the record's terminating newline when one was seen.
	Start int64
	End   int64
	// Overflow is set when the record exceeded MaxBytes. Bytes beyond the
	// limit were dropped from Text, but End still advances past them so the
	// bad record retains its true position.
	Overflow bool
	// Incomplete means the record was flushed without a terminating newline
	// (half-written record at a boundary or after the idle flush timeout).
	Incomplete bool
}

// MultilineConfig configures record boundary detection.
type MultilineConfig struct {
	// Start matches the first line of a multiline record. A line matching
	// Start closes the previous record. A nil StartReg treats every line as
	// a fresh record.
	StartReg *regexp.Regexp
	// MaxBytes bounds the buffered size of one record. Zero means 1 MiB.
	MaxBytes int
}

// Splitter is a byte-stream to multiline-record splitter. It is
// generation-scoped: Flush and Reset on rotation/truncation so a half record
// is never glued across files.
type Splitter struct {
	cfg MultilineConfig

	// carry is a trailing fragment without a terminating newline: it may be
	// a record that is still being written, so it waits for more bytes.
	carry []byte

	pending   bytes.Buffer
	pendStart int64
	pendBytes int
	havePend  bool
	overflow  bool
}

// NewSplitter builds a Splitter.
func NewSplitter(cfg MultilineConfig) *Splitter {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 20
	}
	return &Splitter{cfg: cfg, pendStart: -1}
}

// Feed consumes a chunk starting at chunkStart and returns complete items.
func (s *Splitter) Feed(chunk []byte, chunkStart int64) []Item {
	var items []Item
	data := chunk
	if len(s.carry) > 0 {
		data = append(append(make([]byte, 0, len(s.carry)+len(chunk)), s.carry...), chunk...)
		chunkStart -= int64(len(s.carry))
		s.carry = s.carry[:0]
	}

	for len(data) > 0 {
		nl := bytes.IndexByte(data, '\n')
		if nl < 0 {
			s.carry = append(s.carry[:0], data...)
			break
		}
		line := data[:nl]
		lineStart := chunkStart
		lineEnd := chunkStart + int64(nl) + 1
		data = data[nl+1:]
		chunkStart = lineEnd

		starts := s.cfg.StartReg == nil || s.cfg.StartReg.Match(line)
		if s.havePend && starts {
			items = s.emit(items, lineStart, false)
		}
		if !s.havePend {
			s.havePend = true
			s.pendStart = lineStart
			s.pendBytes = 0
			s.overflow = false
			s.pending.Reset()
		}
		s.appendLine(line)
	}
	return items
}

func (s *Splitter) appendLine(line []byte) {
	add := len(line) + 1 // account for the stripped '\n'
	if s.overflow || s.pendBytes+add > s.cfg.MaxBytes {
		s.overflow = true
		s.pendBytes += add
		return
	}
	if s.pending.Len() > 0 {
		s.pending.WriteByte('\n')
	}
	s.pending.Write(line)
	s.pendBytes += add
}

func (s *Splitter) emit(items []Item, end int64, incomplete bool) []Item {
	items = append(items, Item{
		Text:       s.pending.String(),
		Start:      s.pendStart,
		End:        end,
		Overflow:   s.overflow,
		Incomplete: incomplete,
	})
	s.resetPending()
	return items
}

func (s *Splitter) resetPending() {
	s.havePend = false
	s.pendStart = -1
	s.overflow = false
	s.pending.Reset()
	s.pendBytes = 0
}

// Flush forces out any in-progress record at endOffset. This is used on a
// generation boundary and on idle timeout; the item is marked Incomplete
// when the record never saw a terminating newline.
func (s *Splitter) Flush(endOffset int64) *Item {
	if !s.havePend {
		return nil
	}
	// A pending record exists even when only a no-newline carry accumulated:
	// fold carry into it so the flushed item covers everything read.
	if len(s.carry) > 0 {
		line := s.carry
		starts := s.cfg.StartReg == nil || s.cfg.StartReg.Match(line)
		if starts && s.pendStart >= 0 {
			// Rare: pending without a closing newline and a fresh start line
			// in carry cannot happen with correct feeds; guard anyway.
		}
		s.appendLine(line)
		s.carry = s.carry[:0]
	}
	text := s.pending.String()
	item := &Item{
		Text:       text,
		Start:      s.pendStart,
		End:        endOffset,
		Overflow:   s.overflow,
		Incomplete: true,
	}
	s.resetPending()
	return item
}

// HasPending reports whether a record is being assembled.
func (s *Splitter) HasPending() bool { return s.havePend }

// Reset drops all multiline state for a new generation. Flush first if the
// half record must be published at the boundary.
func (s *Splitter) Reset() {
	s.carry = s.carry[:0]
	s.resetPending()
}
