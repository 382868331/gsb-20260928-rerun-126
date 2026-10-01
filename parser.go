package logengine

import (
	"bytes"
	"encoding/json"
	"regexp"
)

type Line struct {
	Data   []byte
	Start  int64
	End    int64
	Number int
}

// StreamingParser turns a stream of physical lines into parser events.
type StreamingParser interface {
	Process(Line) []Event
	Flush(stream string, id FileID, final bool) []Event
	Reset()
}

type StatefulParser interface {
	StreamingParser
	Capture() any
	Restore(state any)
}

type FileBoundaryParser interface {
	FlushesOnEOF() bool
}

type StartMatcher func([]byte) bool

func RegexpStart(expr *regexp.Regexp) StartMatcher {
	return expr.Match
}

func AlwaysStart(_ []byte) bool { return true }

// MultilineParser joins continuation lines with the following record start.
type MultilineParser struct {
	start       StartMatcher
	maxBytes    int
	pending     []byte
	startOff    int64
	endOff      int64
	startLine   int
	lastLine    int
	has         bool
	next        StreamingParser
	retainAtEOF bool
}

func NewMultilineParser(start StartMatcher, maxBytes int, next StreamingParser) *MultilineParser {
	if start == nil {
		start = AlwaysStart
	}
	return &MultilineParser{start: start, maxBytes: maxBytes, next: next}
}

func (p *MultilineParser) Process(line Line) []Event {
	if !p.has {
		if len(line.Data) > p.maxBytes {
			return p.bad(line, "multiline_record_too_large")
		}
		p.pending = append(p.pending[:0], line.Data...)
		p.startOff, p.endOff = line.Start, line.End
		p.startLine, p.lastLine = line.Number, line.Number
		p.has = true
		return nil
	}

	if p.start(line.Data) {
		events := p.emit()
		p.pending = append(p.pending[:0], line.Data...)
		p.startOff, p.endOff = line.Start, line.End
		p.startLine, p.lastLine = line.Number, line.Number
		p.has = true
		return events
	}

	if len(p.pending)+1+len(line.Data) > p.maxBytes {
		events := []Event{p.badRange(p.startOff, line.End, p.startLine, line.Number, "multiline_record_too_large")}
		p.has = false
		p.pending = p.pending[:0]
		return events
	}
	p.pending = append(p.pending, '\n')
	p.pending = append(p.pending, line.Data...)
	p.endOff = line.End
	p.lastLine = line.Number
	return nil
}

func (p *MultilineParser) Flush(_ string, _ FileID, final bool) []Event {
	if !p.has {
		return nil
	}
	return p.emit()
}

func (p *MultilineParser) Reset() {
	p.has = false
	p.pending = p.pending[:0]
	p.startOff, p.endOff = 0, 0
	p.startLine, p.lastLine = 0, 0
}

func WithMultilineRetainAtEOF(retain bool) func(*MultilineParser) {
	return func(p *MultilineParser) { p.retainAtEOF = retain }
}

func (p *MultilineParser) FlushesOnEOF() bool {
	return !p.retainAtEOF || !p.has
}

func (p *MultilineParser) emit() []Event {
	raw := make([]byte, len(p.pending))
	copy(raw, p.pending)
	record := Record{
		Position: Position{Start: p.startOff, End: p.endOff, Line: p.startLine},
		Raw:      raw,
	}
	if p.next != nil {
		events := p.next.Process(Line{Data: raw, Start: p.startOff, End: p.endOff, Number: p.startLine})
		p.has = false
		p.pending = p.pending[:0]
		return events
	}
	p.has = false
	p.pending = p.pending[:0]
	return []Event{record}
}

func (p *MultilineParser) bad(line Line, reason string) []Event {
	return []Event{p.badRange(line.Start, line.End, line.Number, line.Number, reason)}
}

func (p *MultilineParser) badRange(start, end int64, firstLine, lastLine int, reason string) BadRecord {
	return BadRecord{
		Position: Position{Start: start, End: end, Line: firstLine},
		Reason:   reason,
	}
}

// JSONParser is an example downstream parser and demonstrates parser extension.
type JSONParser struct{}

func NewJSONParser() *JSONParser { return &JSONParser{} }

func (p *JSONParser) Process(line Line) []Event {
	var fields map[string]any
	dec := json.NewDecoder(bytes.NewReader(line.Data))
	if err := dec.Decode(&fields); err != nil {
		return []Event{BadRecord{
			Position: Position{Start: line.Start, End: line.End, Line: line.Number},
			Reason:   "invalid_json",
		}}
	}
	if dec.More() {
		return []Event{BadRecord{
			Position: Position{Start: line.Start, End: line.End, Line: line.Number},
			Reason:   "invalid_json",
		}}
	}
	return []Event{Record{
		Position: Position{Start: line.Start, End: line.End, Line: line.Number},
		Raw:      append([]byte(nil), line.Data...),
		Fields:   fields,
	}}
}

func (p *JSONParser) Flush(_ string, _ FileID, _ bool) []Event { return nil }
func (p *JSONParser) Reset()                                   {}

type multilineState struct {
	Pending   []byte `json:"pending"`
	StartOff  int64  `json:"start_offset"`
	EndOff    int64  `json:"end_offset"`
	StartLine int    `json:"start_line"`
	LastLine  int    `json:"last_line"`
	Has       bool   `json:"has"`
}

func (p *MultilineParser) Capture() any {
	if !p.has {
		return nil
	}
	return multilineState{
		Pending:   append([]byte(nil), p.pending...),
		StartOff:  p.startOff,
		EndOff:    p.endOff,
		StartLine: p.startLine,
		LastLine:  p.lastLine,
		Has:       true,
	}
}

func (p *MultilineParser) Restore(state any) {
	p.Reset()
	if state == nil {
		return
	}
	st, ok := state.(multilineState)
	if !ok {
		return
	}
	p.pending = append(p.pending[:0], st.Pending...)
	p.startOff, p.endOff = st.StartOff, st.EndOff
	p.startLine, p.lastLine = st.StartLine, st.LastLine
	p.has = st.Has
}
