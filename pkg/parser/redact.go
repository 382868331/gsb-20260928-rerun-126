package parser

import (
	"regexp"
)

const maxSampleLen = 512

var (
	// JSON style: "key": "value".
	jsonSecret = regexp.MustCompile(`(?i)("(?:password|passwd|secret|token|api_?key|authorization|auth)"\s*:\s*)"[^"]*"`)
	// key=value (quoted or bare), outside JSON.
	kvEqSecret = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|authorization|auth)\s*=\s*("[^"]*"|'[^']*'|\S+)`)
	// key: value with required whitespace after the colon (so it does not
	// match JSON's "key":... form).
	kvColonSecret = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|authorization|auth)\s*:\s+("[^"]*"|'[^']*'|\S+)`)
	authHeader    = regexp.MustCompile(`(?i)(Authorization\s*[:=]\s*)(Bearer|Basic)\s+\S+`)
)

// Redact returns a length-bounded preview of text with sensitive values
// masked. Key names are preserved; values are replaced with "***".
func Redact(text string) string {
	out := text
	out = authHeader.ReplaceAllString(out, `${1}${2} ***`)
	out = jsonSecret.ReplaceAllString(out, `${1}"***"`)
	out = kvEqSecret.ReplaceAllString(out, `${1}=***`)
	out = kvColonSecret.ReplaceAllString(out, `${1}: ***`)
	if len(out) > maxSampleLen {
		out = out[:maxSampleLen] + "...(truncated)"
	}
	return out
}

// ContainsSensitive reports whether text carries any recognizable secret.
func ContainsSensitive(text string) bool {
	return jsonSecret.MatchString(text) ||
		kvEqSecret.MatchString(text) ||
		kvColonSecret.MatchString(text) ||
		authHeader.MatchString(text)
}
