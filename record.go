package logengine

import (
	"fmt"
	"time"
)

// Position is a byte-exact half-open range in one input stream.
type Position struct {
	Stream     string    `json:"stream"`
	FileID     FileID    `json:"file_id"`
	Start      int64     `json:"start"`
	End        int64     `json:"end"`
	Line       int       `json:"line"`
	OccurredAt time.Time `json:"occurred_at,omitempty"`
}

// EventID is the stable identity used by outputs for idempotent retries.
// It identifies the source byte range, not the payload or parse result, and
// therefore remains stable when the same record is reread after a lost
// checkpoint. OccurredAt is intentionally excluded.
type EventID string

func (p Position) EventID() EventID {
	return EventID(fmt.Sprintf("%s:%d:%d:%d:%d:%d:%d", p.Stream, p.FileID.Device, p.FileID.Inode, p.FileID.ChangeNanos, p.Line, p.Start, p.End))
}

// Record is a valid log record made available to an output.
type Record struct {
	ID       EventID        `json:"id"`
	Position Position       `json:"position"`
	Raw      []byte         `json:"raw,omitempty"`
	Fields   map[string]any `json:"fields,omitempty"`
}

// BadRecord preserves the location of unusable input but intentionally does not
// contain the input itself. That keeps secrets in malformed samples out of
// output logs and dead-letter payloads.
type BadRecord struct {
	ID       EventID  `json:"id"`
	Position Position `json:"position"`
	Reason   string   `json:"reason"`
}

// Event is either a valid record or a redacted bad record.
type Event interface{}

func (r Record) Identity() EventID    { return r.ID }
func (b BadRecord) Identity() EventID { return b.ID }
func (r Record) Pos() Position        { return r.Position }
func (b BadRecord) Pos() Position     { return b.Position }
