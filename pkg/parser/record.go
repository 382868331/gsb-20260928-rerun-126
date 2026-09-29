package parser

// Kind classifies a produced record.
type Kind int

const (
	// KindGood is a fully parsed record.
	KindGood Kind = iota
	// KindBad is a record that no parser accepted. Its sample is redacted and
	// it still carries an exact position so progress is not lost.
	KindBad
)

// Record is one parsed unit with the exact byte range it occupies in the
// originating generation. Positions are generation-local offsets.
type Record struct {
	Kind   Kind
	Source string
	Gen    uint64
	Start  int64
	End    int64

	// Raw is the unredacted multiline text (good records only).
	Raw string
	// Fields holds structured parser output.
	Fields map[string]any

	// RedactedSample is the sensitive-value-redacted preview for bad records
	// and error messages; it never contains raw secret values.
	RedactedSample string
	Reason         string
}
