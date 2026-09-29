package engine

import (
	"sync"

	"logengine/pkg/parser"
)

// MemoryOutput is an in-process Output for tests and embedding: every
// Publish is fully acknowledged once it returns.
type MemoryOutput struct {
	mu      sync.Mutex
	Records []parser.Record
	err     error
}

// SetFail makes subsequent Publish calls return err (nil clears).
func (m *MemoryOutput) SetFail(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

// Publish implements output.Output.
func (m *MemoryOutput) Publish(rec parser.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.Records = append(m.Records, rec)
	return nil
}

// Snapshot returns a copy of acknowledged records.
func (m *MemoryOutput) Snapshot() []parser.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]parser.Record, len(m.Records))
	copy(out, m.Records)
	return out
}
