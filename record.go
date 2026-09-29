package logengine

import "time"

// Position is a byte-exact half-open range in one input stream.
type Position struct {
	Stream     string    `json:"stream"`
	FileID     FileID    `json:"file_id"`
	Start      int64     `json:"start"`
	End        int64     `json:"end"`
	Line       int       `json:"line"`
	OccurredAt time.Time `json:"occurred_at,omitempty"`
}

// Record is a valid log record made available to an output.
type Record struct {
	Position Position
	Raw      []byte
	Fields   map[string]any
}

// BadRecord preserves the location of unusable input but intentionally does not
// contain the input itself. That keeps secrets in malformed samples out of
// output logs and dead-letter payloads.
type BadRecord struct {
	Position Position
	Reason   string
}

// Event is either a valid record or a redacted bad record.
type Event interface{}

func (r Record) Pos() Position    { return r.Position }
func (b BadRecord) Pos() Position { return b.Position }
