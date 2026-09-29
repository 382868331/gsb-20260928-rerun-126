package parser

// Parser attempts to parse one assembled multiline record. Return
// parsed=true when the format is accepted, parsed=false to pass the record
// to the next parser in the chain. Parsers must never mutate the input.
type Parser interface {
	Name() string
	Parse(raw string) (fields map[string]any, parsed bool, err error)
}

// Chain runs parsers in order. A record every parser rejects becomes a
// KindBad record with a redacted sample; a parser error (accepted format,
// invalid content) does the same with the parser's reason.
type Chain struct {
	parsers []Parser
}

// NewChain builds a parser chain. Order matters: the first accepting parser
// wins, so new parsing methods can be appended without touching existing
// ones.
func NewChain(parsers ...Parser) *Chain {
	return &Chain{parsers: parsers}
}

// Parse runs the chain for a raw record.
func (c *Chain) Parse(raw string) (map[string]any, Kind, string) {
	for _, p := range c.parsers {
		fields, ok, err := p.Parse(raw)
		switch {
		case err != nil:
			return nil, KindBad, p.Name() + ": " + err.Error()
		case ok:
			return fields, KindGood, ""
		}
	}
	return nil, KindBad, "no parser matched"
}
