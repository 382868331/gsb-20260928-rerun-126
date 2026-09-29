package parser

import "encoding/json"

// JSONParser accepts records whose body is a JSON object or array, possibly
// preceded by a short non-JSON prefix such as a timestamp.
type JSONParser struct{}

// Name implements Parser.
func (JSONParser) Name() string { return "json" }

// Parse implements Parser.
func (JSONParser) Parse(raw string) (map[string]any, bool, error) {
	b := trimSpace(raw)
	i := indexJSONStart(b)
	if i < 0 {
		return nil, false, nil
	}
	candidate := trimSpace(b[i:])
	var v any
	if err := json.Unmarshal([]byte(candidate), &v); err != nil {
		return nil, true, err
	}
	prefix := b[:i]
	fields := map[string]any{"format": "json", "value": v}
	if prefix != "" {
		fields["prefix"] = prefix
	}
	return fields, true, nil
}

// indexJSONStart finds the first JSON object/array opener that could begin
// a complete JSON document, tolerating timestamp-style prefixes.
func indexJSONStart(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '{' || s[i] == '[' {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && isSpace(s[start]) {
		start++
	}
	for end > start && isSpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\n':
		return true
	}
	return false
}
