// Package engine wires local file tailing, multiline parsing, parser chains
// and output acknowledgement with durable per-record positions.
package engine

import (
	"context"
	"fmt"
	"time"

	"logengine/pkg/output"
	"logengine/pkg/parser"
	"logengine/pkg/position"
	"logengine/pkg/tail"
)

// SourceConfig describes one followed file.
type SourceConfig struct {
	Path      string
	Multiline parser.MultilineConfig
}

// Config configures an Engine.
type Config struct {
	Sources   []SourceConfig
	Store     *position.Store
	Out       output.Output
	Chain     *parser.Chain
	PollEvery time.Duration
	// FlushAfter bounds how long a half-written trailing record may sit
	// unflushed before being published as an incomplete record.
	FlushAfter time.Duration
}

// Engine follows all configured sources concurrently.
type Engine struct {
	cfg Config
}

// New builds an engine with sensible defaults filled in.
func New(cfg Config) *Engine {
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 200 * time.Millisecond
	}
	if cfg.FlushAfter <= 0 {
		cfg.FlushAfter = 5 * time.Second
	}
	if cfg.Chain == nil {
		cfg.Chain = parser.NewChain(parser.JSONParser{})
	}
	return &Engine{cfg: cfg}
}

type source struct {
	cfg      SourceConfig
	tailer   *tail.Tailer
	split    *parser.Splitter
	lastData time.Time
}

// Run loops until ctx is canceled. Each source runs in its own goroutine
// with its own splitter and position cursor.
func (e *Engine) Run(ctx context.Context) error {
	errc := make(chan error, len(e.cfg.Sources))
	for _, sc := range e.cfg.Sources {
		s := &source{cfg: sc, split: parser.NewSplitter(sc.Multiline)}
		go func(s *source) {
			errc <- e.runSource(ctx, s)
		}(s)
	}
	for range e.cfg.Sources {
		if err := <-errc; err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) runSource(ctx context.Context, s *source) error {
	cursor, hasCursor := e.cfg.Store.Get(s.cfg.Path)
	t, err := tail.Open(s.cfg.Path, cursor.Offset, tail.Identity{
		Dev: cursor.Dev, Inode: cursor.Inode, HeadFingerprint: cursor.HeadFingerprint,
	}, hasCursor)
	if err != nil {
		return fmt.Errorf("open %s: %w", s.cfg.Path, err)
	}
	s.tailer = t
	defer t.Close()

	ticker := time.NewTicker(e.cfg.PollEvery)
	defer ticker.Stop()
	flush := time.NewTicker(e.cfg.FlushAfter)
	defer flush.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.tick(s); err != nil {
				return err
			}
		case <-flush.C:
			if err := e.idleFlush(s); err != nil {
				return err
			}
		}
	}
}

func (e *Engine) tick(s *source) error {
	// Poll returns boundary events only after any drained bytes from the old
	// inode have been read. Loop within a tick so drain -> boundary -> drain
	// of the new generation all happen in order without a wait.
	for {
		ev, err := s.tailer.Poll()
		if err != nil {
			return err
		}
		if err := e.drain(s); err != nil {
			return err
		}
		if ev == nil {
			return nil
		}
		if err := e.onBoundary(s, ev); err != nil {
			return err
		}
		// Continue: Poll/Drain the new generation in the same tick.
	}
}

func (e *Engine) drain(s *source) error {
	for {
		b, err := s.tailer.Read()
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		s.lastData = time.Now()
		for _, item := range s.split.Feed(b.Data, b.Start) {
			if err := e.publishItem(s, b, item); err != nil {
				return err
			}
		}
	}
}

func (e *Engine) onBoundary(s *source, ev *tail.Event) error {
	// At a rotation, a dangling no-newline fragment is the last record of
	// the old stream and must not be glued to the new file. At a truncation
	// it belongs to deleted content and is discarded with the reset.
	if s.split.HasPending() {
		end := s.tailer.OldOffset()
		if ev.Kind == tail.EventTruncation {
			s.split.Reset()
			return nil
		}
		item := s.split.Flush(end)
		if item != nil {
			oldGen := s.tailer.PrevGen()
			rec := e.buildRecord(s, *item)
			rec.Gen = oldGen
			if err := e.publish(s, rec); err != nil {
				return err
			}
		}
	}
	s.split.Reset()
	return nil
}

func (e *Engine) idleFlush(s *source) error {
	if !s.split.HasPending() {
		return nil
	}
	if time.Since(s.lastData) < e.cfg.FlushAfter {
		return nil
	}
	item := s.split.Flush(s.tailer.Offset())
	if item == nil {
		return nil
	}
	b := tail.Block{Gen: s.tailer.Gen(), Identity: s.tailer.Identity()}
	return e.publishItem(s, &b, *item)
}

func (e *Engine) publishItem(s *source, b *tail.Block, item parser.Item) error {
	rec := e.buildRecord(s, item)
	if b != nil {
		rec.Gen = b.Gen
	}
	return e.publish(s, rec)
}

func (e *Engine) buildRecord(s *source, item parser.Item) parser.Record {
	fields, kind, reason := e.cfg.Chain.Parse(item.Text)
	rec := parser.Record{
		Kind:   kind,
		Source: s.cfg.Path,
		Start:  item.Start,
		End:    item.End,
		Raw:    item.Text,
		Fields: fields,
		Reason: reason,
	}
	if item.Overflow {
		rec.Kind = parser.KindBad
		rec.Reason = fmt.Sprintf("multiline record exceeds %d bytes", s.cfg.Multiline.MaxBytes)
	}
	if item.Incomplete {
		rec.Reason = appendReason(rec.Reason, "incomplete record at boundary")
	}
	if rec.Kind == parser.KindBad {
		rec.RedactedSample = parser.Redact(item.Text)
		rec.Raw = ""
	} else if parser.ContainsSensitive(item.Text) {
		// Good records must not leak raw secrets either; redact the raw text
		// (structured Fields remain available to authorized consumers).
		rec.Raw = parser.Redact(item.Text)
	}
	rec.Gen = s.tailer.Gen()
	return rec
}

func (e *Engine) publish(s *source, rec parser.Record) error {
	if err := e.cfg.Out.Publish(rec); err != nil {
		return err
	}
	// Commit AFTER publish: restart resumes past this record.
	id := s.tailer.Identity()
	return e.cfg.Store.Commit(position.Cursor{
		Path: s.cfg.Path,
		Dev:  id.Dev, Inode: id.Inode, HeadFingerprint: id.HeadFingerprint, HeadLen: id.HeadLen,
		Offset: rec.End,
	})
}

func appendReason(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}
