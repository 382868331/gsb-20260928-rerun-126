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
	"path/filepath"
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
	stream        string
	path          string
	store         *StateStore
	parser        StreamingParser
	maxLine       int
	readSize      int
	states        map[string]Checkpoint
	file          *os.File
	activePath    string
	id            FileID
	currentSize   int64
	offset        int64
	safeOffset    int64
	nextLine      int
	carry         []byte
	carryStart    int64
	dropOversize  bool
	resetCommit   bool
	boundaryFlush bool
	finishing     bool
	pending       []Event
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

func NewEngine(stream, path, checkpointPath string, parser StreamingParser, opts ...Option) (*Engine, error) {
	store := NewStateStore(checkpointPath)
	states, err := store.Load()
	if err != nil {
		return nil, err
	}
	e, err := newEngine(store, states, stream, path, parser, opts...)
	if err != nil {
		return nil, err
	}
	if err := e.configured(); err != nil {
		return nil, err
	}
	return e, nil
}

func newEngine(store *StateStore, states map[string]Checkpoint, stream, path string, parser StreamingParser, opts ...Option) (*Engine, error) {
	e := &Engine{
		stream:   stream,
		path:     path,
		store:    store,
		parser:   parser,
		maxLine:  1 << 20,
		readSize: 4096,
		states:   states,
	}
	applyOptions(e, opts...)
	if cp, ok := states[stream]; ok && cp.Path != "" && cp.Path != path {
		// A checkpoint belongs to the physical watched path. Reusing a stream
		// name elsewhere must not let one source consume another source's offset.
		cp = Checkpoint{}
		states[stream] = cp
	}
	if err := restoreParser(e, states[stream]); err != nil {
		return nil, err
	}
	return e, nil
}

func applyOptions(e *Engine, opts ...Option) {
	for _, opt := range opts {
		opt(e)
	}
}

func (e *Engine) configured() error {
	if e.maxLine < 1 {
		return errors.New("max line bytes must be positive")
	}
	if e.readSize < 1 {
		return errors.New("read size must be positive")
	}
	return nil
}

func restoreParser(e *Engine, cp Checkpoint) error {
	if sp, ok := e.parser.(StatefulParser); ok && len(cp.ParserState) > 0 {
		var state any
		if _, ok := sp.(*MultilineParser); ok {
			var ms multilineState
			if err := jsonUnmarshal(cp.ParserState, &ms); err != nil {
				return fmt.Errorf("restore parser state: %w", err)
			}
			state = ms
		}
		sp.Restore(state)
	}
	return nil
}

func (e *Engine) Close() error {
	if e.file != nil {
		return e.file.Close()
	}
	return nil
}

func (e *Engine) Pump(output Output) error {
	if err := e.configured(); err != nil {
		return err
	}
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
			id, watchedInfo, activeInfo, activeID, statErr := e.currentPhysicalFile()
			readSafeOffset := e.safeOffset
			if statErr != nil || watchedInfo == nil {
				return 0, statErr
			}
			if activeID.Inode == 0 && samePhysicalFile(id, e.id) && watchedInfo.Size() < e.safeOffset {
				if err := e.resetAfterTruncation(watchedInfo.Size()); err != nil {
					return 0, err
				}
				return 0, errTruncationDetected
			}
			truncationInfo := watchedInfo
			if activeInfo != nil {
				truncationInfo = activeInfo
			}
			if truncationInfo.Size() < e.safeOffset {
				if err := e.resetAfterTruncation(truncationInfo.Size()); err != nil {
					return 0, err
				}
				return 0, errTruncationDetected
			}
			n, readErr := e.file.Read(buf)
			if errors.Is(readErr, io.EOF) && activeID.Inode != 0 && !samePhysicalFile(activeID, id) && activeInfo != nil && activeInfo.Size() <= readSafeOffset+int64(n) {
				return n, errFileRotated
			}
			return n, readErr
		}()
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
		if errors.Is(err, errFileRotated) {
			return e.finishCurrentFile(output)
		}
		if errors.Is(err, errTruncationDetected) {
			return e.publish(output)
		}
		if errors.Is(err, io.EOF) {
			id, info, _, _, statErr := e.currentPhysicalFile()
			if statErr == nil && !samePhysicalFile(id, e.id) {
				return e.finishCurrentFile(output)
			}
			if statErr == nil && samePhysicalFile(id, e.id) && info.Size() < e.safeOffset {
				if err := e.resetAfterTruncation(info.Size()); err != nil {
					return err
				}
				return e.publish(output)
			}
			if samePhysicalFile(id, e.id) {
				if e.shouldFlushAtEOF() {
					if err := e.handleEnd(); err != nil {
						return err
					}
				}
				if e.resetCommit {
					return e.publish(output)
				}
				if len(e.pending) > 0 {
					return e.publish(output)
				}
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

func (e *Engine) shouldFlushAtEOF() bool {
	if e.boundaryFlush {
		return true
	}
	if fp, ok := e.parser.(FileBoundaryParser); ok {
		return fp.FlushesOnEOF()
	}
	return true
}

func (e *Engine) finishCurrentFile(output Output) error {
	oldID := e.id
	oldOffset := e.offset
	oldNextLine := e.nextLine
	var events []Event
	if len(e.carry) > 0 {
		if mp, ok := e.parser.(*MultilineParser); ok && mp.has {
			mp.pending = append(mp.pending, '\n')
			mp.pending = append(mp.pending, e.carry...)
			mp.endOff = e.offset
			mp.lastLine = e.nextLine + 1
			events = append(events, mp.emit()...)
		} else {
			e.nextLine++
			events = append(events, e.parser.Process(Line{Data: append([]byte(nil), e.carry...), Start: e.carryStart, End: e.offset, Number: e.nextLine})...)
		}
		e.carry = e.carry[:0]
	}
	if mp, ok := e.parser.(*MultilineParser); ok && mp.has {
		events = append(events, mp.emit()...)
	}
	if len(events) == 0 {
		events = append(events, e.parser.Flush(e.stream, e.id, true)...)
	}
	for i := range events {
		events[i] = enrichEvent(events[i], e.stream, e.id)
	}
	if len(events) > 0 {
		cp := e.states[e.stream]
		cp.Path = e.path
		cp.FileID = FileID{}
		cp.LastConfirmedFileID = oldID
		cp.Size = oldOffset
		cp.Offset = oldOffset
		cp.NextLine = oldNextLine
		cp.ParserState = nil
		cp.LastRecordID = lastRecordID(events, cp.LastRecordID)
		e.states[e.stream] = cp
		return e.commitFileBoundary(events, output)
	}
	if e.file != nil {
		_ = e.file.Close()
	}
	e.file = nil
	e.activePath = ""
	e.parser.Reset()
	e.offset = 0
	e.safeOffset = 0
	e.nextLine = 0
	e.carryStart = 0
	e.id = FileID{}
	e.currentSize = 0
	e.pending = e.pending[:0]
	e.delivered = false
	e.finishing = false
	e.boundaryFlush = true
	return e.Pump(output)
}

func (e *Engine) commitFileBoundary(events []Event, output Output) error {
	e.pending = append(e.pending, events...)
	if !e.delivered {
		if err := output.Publish(e.pending); err != nil {
			return err
		}
		e.delivered = true
	}
	e.finishing = true
	if err := e.store.Save(e.states); err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointNotSaved, err)
	}
	e.finishAfterBoundaryCommit()
	return e.Pump(output)
}

func (e *Engine) finishAfterBoundaryCommit() {
	if e.file != nil {
		_ = e.file.Close()
	}
	e.file = nil
	e.activePath = ""
	e.parser.Reset()
	e.offset = 0
	e.safeOffset = 0
	e.nextLine = 0
	e.carryStart = 0
	e.id = FileID{}
	e.currentSize = 0
	e.pending = e.pending[:0]
	e.delivered = false
	e.finishing = false
	e.boundaryFlush = true
}

func (e *Engine) ensureOpen() error {
	cp := e.states[e.stream]
	openPath := e.path
	id, info, err := fileIDFromPath(e.path)
	switch {
	case os.IsNotExist(err):
		if cp.FileID.Inode == 0 {
			return nil
		}
		located, found, locateErr := e.findFileByID(cp.FileID)
		if locateErr != nil {
			return locateErr
		}
		if !found {
			return nil
		}
		st, statErr := os.Stat(located)
		if os.IsNotExist(statErr) {
			return nil
		}
		if statErr != nil {
			return statErr
		}
		if st.Size() > cp.Offset {
			openPath = located
			id = cp.FileID
			info = st
		} else if st.Size() < cp.Offset {
			e.markTruncation(cp.FileID, st.Size())
			return e.openCurrentReplacement()
		} else {
			return e.openCurrentReplacement()
		}
	case err != nil:
		return err
	default:
		if cp.FileID.Inode != 0 && !samePhysicalFile(id, cp.FileID) {
			located, found, locateErr := e.findFileByID(cp.FileID)
			if locateErr != nil {
				return locateErr
			}
			if found {
				st, statErr := os.Stat(located)
				if statErr != nil {
					return statErr
				}
				switch {
				case st.Size() > cp.Offset:
					openPath = located
					id = cp.FileID
					info = st
				case st.Size() < cp.Offset:
					e.markTruncation(cp.FileID, st.Size())
					return e.openCurrentReplacement()
				default:
					return e.openCurrentReplacement()
				}
			}
		}
	}

	start := int64(0)
	if info == nil {
		st, statErr := os.Stat(openPath)
		if os.IsNotExist(statErr) {
			return nil
		}
		if statErr != nil {
			return statErr
		}
		info = st
	}
	if samePhysicalFile(id, cp.FileID) && cp.FileID.Inode != 0 {
		if info.Size() < cp.Offset {
			e.markTruncation(id, info.Size())
			return e.openCurrentReplacement()
		}
		start = cp.Offset
		e.nextLine = cp.NextLine
	} else if cp.FileID.Inode != 0 {
		e.parser.Reset()
		e.nextLine = 0
		id.Generation = nextGeneration(cp.FileID, id)
	}

	f, err := os.Open(openPath)
	if err != nil {
		return err
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	e.file = f
	e.activePath = openPath
	e.id = id
	e.currentSize = info.Size()
	e.offset = start
	e.safeOffset = start
	e.carryStart = start
	return nil
}

func (e *Engine) currentPhysicalFile() (FileID, os.FileInfo, os.FileInfo, FileID, error) {
	watchedID, watchedInfo, watchedErr := fileIDFromPath(e.path)
	if watchedErr != nil && !os.IsNotExist(watchedErr) {
		return FileID{}, nil, nil, FileID{}, watchedErr
	}
	if e.file != nil && e.activePath != "" && e.activePath != e.path {
		if info, err := os.Stat(e.activePath); err == nil && info.Size() > e.safeOffset {
			id, idErr := fileIDFromInfo(info)
			if idErr == nil && id.Device == e.id.Device && id.Inode == e.id.Inode {
				id.Generation = e.id.Generation
				if watchedErr == nil {
					return watchedID, watchedInfo, info, id, nil
				}
				return id, info, info, id, nil
			}
		}
	}
	if watchedErr == nil {
		return watchedID, watchedInfo, nil, FileID{}, nil
	}
	if e.activePath != "" && e.activePath != e.path {
		id, info, err := fileIDFromPath(e.activePath)
		if err == nil {
			if id.Device == e.id.Device && id.Inode == e.id.Inode {
				id.Generation = e.id.Generation
			}
			return id, info, info, id, nil
		}
		if !os.IsNotExist(err) {
			return FileID{}, nil, nil, FileID{}, err
		}
	}
	return FileID{}, nil, nil, FileID{}, nil
}

func (e *Engine) findFileByID(want FileID) (string, bool, error) {
	wantGeneration := want.Generation
	want.Generation = 0
	matches := func(path string, d os.DirEntry) bool {
		if d.IsDir() {
			return false
		}
		id, err := fileIDFromDirEntry(path, d)
		if err != nil {
			return false
		}
		id.Generation = 0
		return id == want
	}
	dir := filepath.Dir(e.path)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if matches(path, entry) {
			_ = wantGeneration
			return path, true, nil
		}
	}
	return "", false, nil
}

func (e *Engine) openCurrentReplacement() error {
	id, info, err := fileIDFromPath(e.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	cp := e.states[e.stream]
	id.Generation = nextGeneration(cp.FileID, id)
	f, err := os.Open(e.path)
	if err != nil {
		return err
	}
	e.file = f
	e.activePath = e.path
	e.id = id
	e.currentSize = info.Size()
	e.offset = 0
	e.safeOffset = 0
	e.nextLine = 0
	e.carryStart = 0
	cp.FileID = id
	cp.Path = e.path
	cp.Size = info.Size()
	cp.Offset = 0
	cp.NextLine = 0
	cp.ParserState = nil
	cp.LastRecordID = ""
	e.states[e.stream] = cp
	e.boundaryFlush = true
	return nil
}

func (e *Engine) markTruncation(id FileID, currentSize int64) {
	cp := e.states[e.stream]
	e.pending = append(e.pending, BadRecord{
		Position: Position{Stream: e.stream, FileID: id, Start: cp.Offset, End: currentSize, Line: cp.NextLine},
		Reason:   "file_truncated",
	})
	cp.Size = currentSize
	e.states[e.stream] = cp
	e.parser.Reset()
}

func (e *Engine) resetAfterTruncation(currentSize int64) error {
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
	newID := e.id
	newID.Generation = e.id.Generation + 1
	cp.FileID = newID
	cp.Size = currentSize
	cp.Offset = 0
	cp.NextLine = 0
	cp.ParserState = nil
	e.states[e.stream] = cp
	e.id = newID
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
	if samePhysicalFile(id, e.id) {
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
	cp.Path = e.path
	cp.FileID = e.id
	cp.Size = e.currentSize
	cp.Offset = e.safeOffset
	cp.NextLine = e.nextLine
	cp.LastRecordID = lastRecordID(nil, cp.LastRecordID)
	e.boundaryFlush = false
	if sp, ok := e.parser.(StatefulParser); ok {
		raw, err := jsonMarshal(sp.Capture())
		if err != nil {
			return err
		}
		cp.ParserState = raw
	}
	e.states[e.stream] = cp
	return e.store.Save(e.states)
}

func (e *Engine) ack(events []Event) error {
	if e.finishing {
		if err := e.store.Save(e.states); err != nil {
			return err
		}
		e.finishAfterBoundaryCommit()
		return nil
	}
	if e.resetCommit {
		cp := e.states[e.stream]
		cp.Path = e.path
		cp.FileID = e.id
		cp.Offset = 0
		cp.NextLine = 0
		cp.ParserState = nil
		cp.LastRecordID = ""
		e.states[e.stream] = cp
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

	cp := e.states[e.stream]
	cp.Path = e.path
	cp.FileID = e.id
	cp.Size = e.currentSize
	cp.Offset = e.safeOffset
	cp.NextLine = e.nextLine
	cp.LastRecordID = lastRecordID(events, cp.LastRecordID)
	e.boundaryFlush = false
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
	if err := e.store.Save(e.states); err != nil {
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

func nextGeneration(previous, current FileID) uint64 {
	if previous.Device == current.Device && previous.Inode == current.Inode {
		return previous.Generation + 1
	}
	return 0
}

func samePhysicalFile(a, b FileID) bool {
	return a.Device == b.Device && a.Inode == b.Inode
}

func lastRecordID(events []Event, fallback string) string {
	for i := len(events) - 1; i >= 0; i-- {
		if record, ok := events[i].(Record); ok {
			return record.ID
		}
	}
	return fallback
}

func indexNewline(data []byte) int {
	for i, b := range data {
		if b == '\n' {
			return i
		}
	}
	return -1
}
