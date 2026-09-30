package logengine

import (
	"errors"
	"io"
	"os"
)

var (
	errFileRotated        = errors.New("file rotated")
	errTruncationDetected = errors.New("truncation detected")
)

type Output interface {
	// Publish receives a batch with stable event IDs. It must be idempotent:
	// after it has returned nil once, an engine crash before the checkpoint is
	// cleared can cause the same batch and IDs to be retried after restart.
	// A returned error is treated as uncertain, including if the write reached
	// the receiver; the engine cannot infer atomic completion of two separate
	// systems, so receivers must dedupe instead of creating a second business
	// record for an already-applied ID.
	Publish(events []Event) error
}

type Engine struct {
	stream       string
	path         string
	store        checkpointStore
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
	pendingCP    Checkpoint
}

type Option func(*Engine)

type checkpointStore interface {
	Load() (map[string]Checkpoint, error)
	Save(states map[string]Checkpoint) error
}

func WithMaxLineBytes(n int) Option {
	return func(e *Engine) { e.maxLine = n }
}

func WithReadSize(n int) Option {
	return func(e *Engine) { e.readSize = n }
}

func WithCheckpointStore(store checkpointStore) Option {
	return func(e *Engine) { e.store = store }
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
		store:    checkpointStore(store),
		parser:   parser,
		maxLine:  1 << 20,
		readSize: 4096,
		states:   states,
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.store == nil {
		e.store = store
	} else if e.store != store {
		states, err = e.store.Load()
		if err != nil {
			return nil, err
		}
		e.states = states
	}
	if e.maxLine < 1 {
		return nil, errors.New("max line bytes must be positive")
	}
	if e.readSize < 1 {
		return nil, errors.New("read size must be positive")
	}
	if cp, ok := states[stream]; ok {
		if cp.Pending != nil {
			e.pending = restoreEvents(cp.Pending.Events)
			e.pendingCP = cp
			if sp, ok := parser.(StatefulParser); ok && len(cp.Pending.ParserState) > 0 {
				if err := restoreMultilineState(sp, cp.Pending.ParserState); err != nil {
					return nil, err
				}
			}
		} else if sp, ok := parser.(StatefulParser); ok && len(cp.ParserState) > 0 {
			if err := restoreMultilineState(sp, cp.ParserState); err != nil {
				return nil, err
			}
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
			if statErr == nil && !id.SameFile(e.id) {
				return 0, errFileRotated
			}
			if statErr == nil && id.SameFile(e.id) && info.Size() < e.safeOffset {
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
			if statErr == nil && !id.SameFile(e.id) {
				return e.finishCurrentFile(output)
			}
			if statErr == nil && id.SameFile(e.id) && info.Size() < e.safeOffset {
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
	if cp.FileID.SameFile(id) {
		if info.Size() < cp.Offset {
			return e.resetAfterTruncation(info.Size())
		}
		start = cp.Offset
		e.nextLine = cp.NextLine
	} else if cp.Offset > 0 || cp.FileID.Inode != 0 {
		// A checkpoint from a different inode must never be continued as though
		// the replacement file were the second half of the old file.
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
	e.parser.Reset()
	e.pending = append(e.pending, BadRecord{
		Position: Position{Stream: e.stream, Start: e.states[e.stream].Offset, End: currentSize, Line: e.states[e.stream].NextLine},
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
	if id.SameFile(e.id) {
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
	if e.pendingCP.Pending == nil {
		if err := e.stagePending(); err != nil {
			return err
		}
	}
	if err := output.Publish(e.pending); err != nil {
		return err
	}
	return e.ack(e.pending)
}

func (e *Engine) saveReadPosition() error {
	cp := e.states[e.stream]
	cp.FileID = e.id
	cp.Offset = e.safeOffset
	cp.NextLine = e.nextLine
	raw, err := parserStateForCheckpoint(e.parser)
	if err != nil {
		return err
	}
	cp.ParserState = raw
	cp.Pending = nil
	e.states[e.stream] = cp
	return e.store.Save(e.states)
}

func (e *Engine) stagePending() error {
	raw, err := parserStateForCheckpoint(e.parser)
	if err != nil {
		return err
	}
	cp := e.states[e.stream]
	cp.Pending = &PendingBatch{
		FileID:      e.id,
		Offset:      e.safeOffset,
		NextLine:    e.nextLine,
		ParserState: raw,
		Reset:       e.resetCommit,
		Events:      persistEvents(e.pending),
	}
	e.states[e.stream] = cp
	e.pendingCP = cp
	return e.store.Save(e.states)
}

func (e *Engine) ack(_ []Event) error {
	cp, ok := e.states[e.stream]
	if !ok || cp.Pending == nil {
		cp = e.pendingCP
	}
	if cp.Pending != nil {
		reset := cp.Pending.Reset
		if reset {
			cp = Checkpoint{}
		} else {
			cp = committedCheckpointFromPending(cp)
		}
		e.states[e.stream] = cp
		if err := e.store.Save(e.states); err != nil {
			return err
		}
		if reset {
			e.id = FileID{}
			e.offset = 0
			e.safeOffset = 0
			e.nextLine = 0
		}
		e.resetCommit = false
		e.pendingCP = Checkpoint{}
		e.pending = e.pending[:0]
		return nil
	}

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
		if err := e.store.Save(e.states); err != nil {
			return err
		}
		e.pending = e.pending[:0]
		return nil
	}

	committedCP := e.states[e.stream]
	committedCP.FileID = e.id
	committedCP.Offset = e.safeOffset
	committedCP.NextLine = e.nextLine
	raw, err := parserStateForCheckpoint(e.parser)
	if err != nil {
		return err
	}
	committedCP.ParserState = raw
	e.states[e.stream] = committedCP
	if err := e.store.Save(e.states); err != nil {
		return err
	}
	e.pending = e.pending[:0]
	return nil
}

func enrichEvent(event Event, stream string, id FileID) Event {
	switch v := event.(type) {
	case Record:
		v.Position.Stream = stream
		v.Position.FileID = id
		v.ID = v.Position.EventID()
		return v
	case BadRecord:
		v.Position.Stream = stream
		v.Position.FileID = id
		v.ID = v.Position.EventID()
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
