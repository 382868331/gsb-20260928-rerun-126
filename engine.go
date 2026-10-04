// Package logengine tails log files and hands parsed records to an Output.
//
// Delivery and acknowledgement semantics:
//
//   - Publishing records and persisting the read checkpoint are two INDEPENDENT
//     writes to two different systems. There is no ordering in which the pair
//     commits atomically, and this package never claims otherwise.
//   - On each batch the call order is strictly: read -> Publish -> persist
//     checkpoint. The checkpoint advances only after Publish returns nil.
//   - If Publish fails, nothing is acknowledged: the same records are read and
//     published again on the next Pump (after restart or within the process).
//   - If Publish succeeds but the checkpoint write fails, the records are
//     already delivered and MUST NOT be published a second time within this
//     process; Pump keeps them pending and only retries the checkpoint write.
//   - After a crash in that window the checkpoint is stale, so recovery reads
//     and republishes the tail. Every Record therefore carries a stable ID
//     (see [Record.ID]) derived from stream, file identity and byte range, and
//     every Output MUST use that ID as its idempotency key so a redelivery
//     never creates a second business record.
//
// Rotation is detected by (device, inode): a path replaced by a different file
// is read from offset zero of the new file. Truncation is reported as a
// redacted BadRecord carrying only the position and reason, and reading
// restarts at zero of the truncated file.
package logengine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

var (
	errFileRotated        = errors.New("file rotated")
	errTruncationDetected = errors.New("truncation detected")

	// ErrCheckpointNotSaved wraps a checkpoint persistence failure that happens
	// after Publish has already succeeded. The records are delivered; callers
	// must keep the engine alive and retry Pump rather than treat this as a
	// publish failure.
	ErrCheckpointNotSaved = errors.New("checkpoint not saved")
)

type Output interface {
	// Publish makes a batch of records durable at the receiver. It MUST be
	// idempotent with respect to Record.ID: the engine provides at-least-once
	// delivery and may present the same ID more than once across retries.
	// Returning an error means the batch is not confirmed; nothing will be
	// acknowledged and the batch will be retried. The input slice is owned by
	// the engine and must not be retained.
	Publish(events []Event) error
}

type Engine struct {
	stream       string
	path         string
	store        *StateStore
	consumer     string
	cstore       *ConsumerStore
	parser       StreamingParser
	maxLine      int
	readSize     int
	states       map[string]Checkpoint
	file         *os.File
	id           FileID
	offset       int64
	safeOffset   int64
	nextLine     int
	carry        []byte
	carryStart   int64
	dropOversize bool
	resetCommit  bool
	pending      []Event
	// delivered is true once Publish has succeeded for pending but the
	// checkpoint save has not yet confirmed. While true, Pump retries only the
	// checkpoint write instead of publishing the same records again.
	delivered bool
}

type Option func(*Engine)

func WithMaxLineBytes(n int) Option {
	return func(e *Engine) { e.maxLine = n }
}

func WithReadSize(n int) Option {
	return func(e *Engine) { e.readSize = n }
}

// WithConsumer attaches the engine to a persistent consumer identity. The
// consumer's confirmed checkpoint is tracked independently of every other
// consumer: confirming a batch advances only this consumer. Consumers that
// have not explicitly Joined start at JoinOldest.
func WithConsumer(name string, store *ConsumerStore) Option {
	return func(e *Engine) {
		e.consumer = name
		e.cstore = store
	}
}

func NewEngine(stream, path, checkpointPath string, parser StreamingParser, opts ...Option) (*Engine, error) {
	store := NewStateStore(checkpointPath)
	states, err := store.Load()
	if err != nil {
		return nil, err
	}
	e := &Engine{
		stream:   stream,
		path:     path,
		store:    store,
		parser:   parser,
		maxLine:  1 << 20,
		readSize: 4096,
		states:   states,
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.cstore != nil {
		if e.consumer == "" {
			return nil, errors.New("consumer name must not be empty")
		}
		cs, err := e.cstore.ensureConsumer(e.consumer)
		if err != nil {
			return nil, err
		}
		e.states = cs.Checkpoints
	}
	if e.maxLine < 1 {
		return nil, errors.New("max line bytes must be positive")
	}
	if e.readSize < 1 {
		return nil, errors.New("read size must be positive")
	}
	if cp, ok := e.states[stream]; ok {
		if sp, ok := parser.(StatefulParser); ok && len(cp.ParserState) > 0 {
			var state any
			// MultilineParser owns the concrete state shape in this first version.
			if _, ok := sp.(*MultilineParser); ok {
				var ms multilineState
				if err := jsonUnmarshal(cp.ParserState, &ms); err != nil {
					return nil, fmt.Errorf("restore parser state: %w", err)
				}
				state = ms
			}
			sp.Restore(state)
		}
	}
	return e, nil
}

func (e *Engine) Close() error {
	if e.file != nil {
		return e.file.Close()
	}
	return nil
}

// RecoveryStatus reconciles the persisted read position with the receiver's
// confirmations after (re)opening a source.
//
// The checkpoint is advanced only after the Output confirms a batch, so
// Confirmed marks everything the receiver has acknowledged. Anything in the
// current file beyond Confirmed.Offset (UnconfirmedBytes) will be published
// now; if the process previously crashed between a successful Publish and the
// checkpoint write, part of that range was already delivered and will arrive
// a second time with the SAME Record.ID. Receivers must deduplicate on that
// ID; the engine itself never knows which prefix of the range was delivered.
type RecoveryStatus struct {
	Stream string
	// Confirmed is the highest receiver-confirmed checkpoint for the stream.
	Confirmed Checkpoint
	// SameFile reports whether the path still holds the confirmed file. When
	// false the file was rotated or recreated while collection was paused and
	// reading restarts at offset zero of the new file.
	SameFile bool
	// UnconfirmedBytes is the amount of data in the current file that is not
	// covered by Confirmed and is therefore subject to (re)delivery.
	UnconfirmedBytes int64
	// Gaps lists ranges of this stream that became unavailable while the
	// consumer was away (rotation or truncation). Only populated when the
	// engine is attached to a consumer identity.
	Gaps []Gap
	// Abandoned lists ranges this consumer deliberately gave up; they are
	// not part of Confirmed and were never processed successfully.
	Abandoned []Abandonment
}

// RecoveryStatus reports what is confirmed and what may be republished for
// this engine's stream, without reading or publishing anything.
func (e *Engine) RecoveryStatus() RecoveryStatus {
	status := RecoveryStatus{Stream: e.stream, Confirmed: e.states[e.stream]}
	id, info, err := fileIDFromPath(e.path)
	if err != nil {
		return status
	}
	// A zero checkpoint means nothing was ever confirmed: the file is a fresh
	// start, not a rotation away from a confirmed file.
	status.SameFile = status.Confirmed.FileID.Inode == 0 || id == status.Confirmed.FileID
	if status.SameFile {
		if size := info.Size(); size > status.Confirmed.Offset {
			status.UnconfirmedBytes = size - status.Confirmed.Offset
		}
	} else {
		status.UnconfirmedBytes = info.Size()
	}
	if e.cstore != nil {
		if gaps, err := e.cstore.Gaps(e.stream); err == nil {
			status.Gaps = gaps
		}
		if abandoned, err := e.cstore.Abandonments(e.consumer); err == nil {
			for _, ab := range abandoned {
				if ab.Stream == e.stream {
					status.Abandoned = append(status.Abandoned, ab)
				}
			}
		}
	}
	return status
}

// Abandon gives up the unconfirmed byte range of this stream up to and
// including offset through, relieving retention pressure on the source. The
// give-up is recorded as an Abandonment carrying the range and reason; it is
// persisted separately from the confirmed checkpoint so it can never be
// mistaken for successfully processed work. The consumer's checkpoint moves
// to through, so the abandoned range is not read again.
func (e *Engine) Abandon(through int64, reason string) error {
	if e.cstore == nil {
		return errors.New("abandon requires a consumer identity")
	}
	if reason == "" {
		return errors.New("abandon reason must not be empty")
	}
	cp := e.states[e.stream]
	if through <= cp.Offset {
		return fmt.Errorf("abandon offset %d does not exceed confirmed offset %d", through, cp.Offset)
	}
	from := cp.Offset
	// A record still pending in the parser is unconfirmed too; giving up
	// history silently dropping it would disguise the give-up as progress.
	if sp, ok := e.parser.(StatefulParser); ok {
		if ms, ok := sp.Capture().(multilineState); ok && ms.Has && ms.StartOff < from {
			from = ms.StartOff
		}
	}
	// Records already read but not yet confirmed by the output are part of
	// the given-up history as well.
	if len(e.pending) > 0 {
		if start := eventPosition(e.pending[0]).Start; start < from {
			from = start
		}
	}
	ab := Abandonment{
		Stream: e.stream,
		FileID: cp.FileID,
		From:   from,
		To:     through,
		Reason: reason,
		At:     time.Now(),
	}
	cp.Offset = through
	// Line numbering past a skipped range is unknowable without reading it;
	// record identity never depends on it (see stableRecordID).
	cp.NextLine = 0
	cp.ParserState = nil
	if err := e.cstore.abandon(e.consumer, ab, cp); err != nil {
		return err
	}
	e.states[e.stream] = cp
	if e.file != nil && e.id == cp.FileID && e.safeOffset < through {
		if _, err := e.file.Seek(through, io.SeekStart); err != nil {
			return err
		}
		e.offset = through
		e.safeOffset = through
		e.carry = e.carry[:0]
		e.carryStart = through
		e.nextLine = 0
		e.parser.Reset()
	}
	return nil
}

func (e *Engine) Pump(output Output) error {
	if len(e.pending) > 0 {
		return e.publish(output)
	}
	if err := e.ensureOpen(); err != nil {
		return err
	}
	if e.resetCommit {
		return e.publish(output)
	}
	if e.file == nil {
		return nil
	}
	currentOffset, err := e.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if currentOffset == e.safeOffset && e.safeOffset > e.offset {
		e.offset = e.safeOffset
	}
	if currentOffset < e.safeOffset {
		if _, err := e.file.Seek(e.safeOffset, io.SeekStart); err != nil {
			return err
		}
	}

	buf := make([]byte, e.readSize)
	for {
		n, err := func() (int, error) {
			id, info, statErr := fileIDFromPath(e.path)
			if statErr != nil && !os.IsNotExist(statErr) {
				return 0, statErr
			}
			if statErr == nil && id != e.id {
				return 0, errFileRotated
			}
			if statErr == nil && id == e.id && info.Size() < e.safeOffset {
				if err := e.resetAfterTruncation(info.Size()); err != nil {
					return 0, err
				}
				return 0, errTruncationDetected
			}
			return e.file.Read(buf)
		}()
		if errors.Is(err, errFileRotated) {
			return e.finishCurrentFile(output)
		}
		if errors.Is(err, errTruncationDetected) {
			return e.publish(output)
		}
		if n > 0 {
			events, lineEnd, fatal := e.consume(buf[:n])
			if fatal != nil {
				return fatal
			}
			e.offset = lineEnd
			if len(events) > 0 {
				e.pending = append(e.pending, events...)
				return e.publish(output)
			}
		}
		if errors.Is(err, io.EOF) {
			id, info, statErr := fileIDFromPath(e.path)
			if statErr == nil && id != e.id {
				return e.finishCurrentFile(output)
			}
			if statErr == nil && id == e.id && info.Size() < e.safeOffset {
				if err := e.resetAfterTruncation(info.Size()); err != nil {
					return err
				}
				return e.publish(output)
			}
			if err := e.handleEnd(); err != nil {
				return err
			}
			if e.resetCommit {
				return e.publish(output)
			}
			if len(e.pending) > 0 {
				return e.publish(output)
			}
			if e.offset > e.safeOffset {
				if _, err := e.file.Seek(e.safeOffset, io.SeekStart); err != nil {
					return err
				}
				e.offset = e.safeOffset
				e.carry = e.carry[:0]
				e.carryStart = e.safeOffset
			}
			if err := e.saveReadPosition(); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (e *Engine) finishCurrentFile(output Output) error {
	var events []Event
	if len(e.carry) > 0 {
		e.nextLine++
		events = append(events, e.parser.Process(Line{Data: append([]byte(nil), e.carry...), Start: e.carryStart, End: e.offset, Number: e.nextLine})...)
		e.carry = e.carry[:0]
	}
	events = append(events, e.parser.Flush(e.stream, e.id, true)...)
	for i := range events {
		events[i] = enrichEvent(events[i], e.stream, e.id)
	}
	if e.file != nil {
		_ = e.file.Close()
	}
	e.file = nil
	e.parser.Reset()
	e.offset = 0
	e.safeOffset = 0
	e.nextLine = 0
	e.carryStart = 0
	e.id = FileID{}
	if len(events) > 0 {
		e.pending = append(e.pending, events...)
		if err := e.publish(output); err != nil {
			return err
		}
	}
	return e.Pump(output)
}

func (e *Engine) ensureOpen() error {
	id, info, err := fileIDFromPath(e.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	cp := e.states[e.stream]
	start := int64(0)
	if cp.FileID == id {
		if info.Size() < cp.Offset {
			return e.resetAfterTruncation(info.Size())
		}
		start = cp.Offset
		e.nextLine = cp.NextLine
	} else if cp.Offset > 0 || cp.FileID.Inode != 0 {
		e.noteGap(cp.FileID, cp.Offset, -1, "rotated_away")
		// A checkpoint from a different inode must never be continued as though
		// the replacement file were the second half of the old file.
		//
		// The old file may have ended with an incomplete multiline record whose
		// parser state was restored from the checkpoint. Finalize it against
		// the OLD file identity and deliver it as its own record instead of
		// silently dropping it or, worse, joining it to the head of the new
		// file. Its Record.ID carries the old inode, so receivers can still
		// deduplicate it against any pre-rotation delivery.
		events := e.parser.Flush(e.stream, cp.FileID, true)
		for i := range events {
			events[i] = enrichEvent(events[i], e.stream, cp.FileID)
		}
		e.pending = append(e.pending, events...)
		start = 0
		e.parser.Reset()
		e.nextLine = 0
	}

	f, err := os.Open(e.path)
	if err != nil {
		return err
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	e.file, e.id, e.offset, e.safeOffset = f, id, start, start
	e.carryStart = start
	return nil
}

func (e *Engine) resetAfterTruncation(currentSize int64) error {
	e.noteGap(e.states[e.stream].FileID, e.states[e.stream].Offset, -1, "file_truncated")
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	e.parser.Reset()
	e.pending = append(e.pending, BadRecord{
		Position: Position{Stream: e.stream, FileID: e.id, Start: e.states[e.stream].Offset, End: currentSize, Line: e.states[e.stream].NextLine},
		Reason:   "file_truncated",
	})
	cp := e.states[e.stream]
	cp.FileID = FileID{}
	cp.Offset = 0
	cp.NextLine = 0
	cp.ParserState = nil
	e.states[e.stream] = cp
	e.resetCommit = true
	return nil
}

func (e *Engine) consume(chunk []byte) ([]Event, int64, error) {
	var events []Event
	lineEnd := e.offset
	for len(chunk) > 0 {
		newline := indexNewline(chunk)
		if newline < 0 {
			if e.dropOversize {
				lineEnd += int64(len(chunk))
				return events, lineEnd, nil
			}
			if len(e.carry)+len(chunk) > e.maxLine {
				end := e.offset + int64(len(chunk))
				events = append(events, BadRecord{
					Position: Position{Stream: e.stream, FileID: e.id, Start: e.carryStart, End: end, Line: e.nextLine + 1},
					Reason:   "line_too_large",
				})
				e.carry = e.carry[:0]
				e.carryStart = end
				e.dropOversize = true
				lineEnd = end
				return events, lineEnd, nil
			}
			e.carry = append(e.carry, chunk...)
			lineEnd += int64(len(chunk))
			return events, lineEnd, nil
		}

		part := chunk[:newline]
		consumed := newline + 1
		if e.dropOversize {
			chunk = chunk[consumed:]
			lineEnd += int64(consumed)
			e.dropOversize = false
			e.carryStart = lineEnd
			continue
		}
		e.carry = append(e.carry, part...)
		if len(e.carry) > e.maxLine {
			events = append(events, BadRecord{
				Position: Position{Stream: e.stream, FileID: e.id, Start: e.carryStart, End: e.offset + int64(consumed), Line: e.nextLine + 1},
				Reason:   "line_too_large",
			})
		} else {
			e.nextLine++
			lineEvents := e.parser.Process(Line{
				Data:   append([]byte(nil), e.carry...),
				Start:  e.carryStart,
				End:    e.offset + int64(consumed),
				Number: e.nextLine,
			})
			events = append(events, lineEvents...)
		}
		for i := range events {
			events[i] = enrichEvent(events[i], e.stream, e.id)
		}
		e.carry = e.carry[:0]
		e.carryStart = e.offset + int64(consumed)
		chunk = chunk[consumed:]
		e.offset += int64(consumed)
		e.safeOffset = e.offset
		lineEnd = e.offset
	}
	return events, lineEnd, nil
}

func (e *Engine) handleEnd() error {
	id, info, err := fileIDFromPath(e.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if id == e.id {
		if info.Size() < e.safeOffset {
			return e.resetAfterTruncation(info.Size())
		}
		events := e.parser.Flush(e.stream, e.id, true)
		for i := range events {
			events[i] = enrichEvent(events[i], e.stream, e.id)
		}
		if len(events) > 0 {
			e.pending = append(e.pending, events...)
		}
		return nil
	}
	return nil
}

func (e *Engine) publish(output Output) error {
	if !e.delivered {
		// Publish and checkpoint persist are independent writes. A failure here
		// means nothing was confirmed: keep the batch pending and let the next
		// Pump (or a restarted process) read and publish it again.
		if err := output.Publish(e.pending); err != nil {
			return err
		}
		// The output is now confirmed. If the checkpoint write below fails we
		// must not publish this batch again in-process; recovery relies on the
		// stable Record.ID and receiver-side deduplication instead.
		e.delivered = true
	}
	if err := e.ack(e.pending); err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointNotSaved, err)
	}
	e.delivered = false
	e.pending = e.pending[:0]
	return nil
}

func (e *Engine) saveReadPosition() error {
	cp := e.states[e.stream]
	cp.FileID = e.id
	cp.Offset = e.safeOffset
	cp.NextLine = e.nextLine
	if sp, ok := e.parser.(StatefulParser); ok {
		raw, err := jsonMarshal(sp.Capture())
		if err != nil {
			return err
		}
		cp.ParserState = raw
	}
	e.states[e.stream] = cp
	return e.saveStream(cp)
}

// saveStream persists the confirmed checkpoint through the consumer store
// when a consumer identity is attached, so progress is tracked per consumer;
// otherwise it falls back to the shared per-stream checkpoint file.
func (e *Engine) saveStream(cp Checkpoint) error {
	if e.cstore != nil {
		return e.cstore.SaveStream(e.consumer, e.stream, cp)
	}
	return e.store.SaveStream(e.stream, cp)
}

// noteGap records an unavailable range for this consumer's stream. Gaps are
// informational; a failure to record one must not interrupt collection.
func (e *Engine) noteGap(id FileID, from, to int64, reason string) {
	if e.cstore == nil || id.Inode == 0 {
		return
	}
	_ = e.cstore.noteGap(Gap{
		Consumer: e.consumer,
		Stream:   e.stream,
		FileID:   id,
		From:     from,
		To:       to,
		Reason:   reason,
		At:       time.Now(),
	})
}

func (e *Engine) ack(events []Event) error {
	if e.resetCommit {
		cp := e.states[e.stream]
		cp.FileID = FileID{}
		cp.Offset = 0
		cp.NextLine = 0
		cp.ParserState = nil
		e.states[e.stream] = cp
		e.id = FileID{}
		e.offset = 0
		e.safeOffset = 0
		e.nextLine = 0
		e.resetCommit = false
		if err := e.saveStream(cp); err != nil {
			return err
		}
		e.pending = e.pending[:0]
		return nil
	}

	cp := e.states[e.stream]
	cp.FileID = e.id
	cp.Offset = e.safeOffset
	cp.NextLine = e.nextLine
	if sp, ok := e.parser.(StatefulParser); ok {
		raw, err := jsonMarshal(sp.Capture())
		if err != nil {
			return err
		}
		cp.ParserState = raw
	} else {
		cp.ParserState = nil
	}
	e.states[e.stream] = cp
	if err := e.saveStream(cp); err != nil {
		return err
	}
	e.delivered = false
	e.pending = e.pending[:0]
	return nil
}

func enrichEvent(event Event, stream string, id FileID) Event {
	switch v := event.(type) {
	case Record:
		v.Position.Stream = stream
		v.Position.FileID = id
		v.ID = stableRecordID(stream, id, v.Position.Start, v.Position.End)
		return v
	case BadRecord:
		v.Position.Stream = stream
		v.Position.FileID = id
		return v
	}
	return event
}

func eventPosition(event Event) Position {
	switch v := event.(type) {
	case Record:
		return v.Position
	case BadRecord:
		return v.Position
	default:
		return Position{}
	}
}

func indexNewline(data []byte) int {
	for i, b := range data {
		if b == '\n' {
			return i
		}
	}
	return -1
}
