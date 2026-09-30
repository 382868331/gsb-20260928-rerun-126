package logengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Checkpoint struct {
	FileID      FileID          `json:"file_id"`
	Offset      int64           `json:"offset"`
	NextLine    int             `json:"next_line"`
	ParserState json.RawMessage `json:"parser_state,omitempty"`
	Pending     *PendingBatch   `json:"pending,omitempty"`
}

type PendingBatch struct {
	FileID      FileID           `json:"file_id"`
	Offset      int64            `json:"offset"`
	NextLine    int              `json:"next_line"`
	ParserState json.RawMessage  `json:"parser_state,omitempty"`
	Reset       bool             `json:"reset,omitempty"`
	Events      []PersistedEvent `json:"events"`
}

type PersistedEvent struct {
	Kind     string         `json:"kind"`
	ID       EventID        `json:"id"`
	Position Position       `json:"position"`
	Reason   string         `json:"reason,omitempty"`
	Raw      []byte         `json:"raw,omitempty"`
	Fields   map[string]any `json:"fields,omitempty"`
}

// StateStore stores the highest fully confirmed byte offset for each stream.
// A non-nil Pending marks the two-phase output boundary: the batch is durable
// locally and may be sent, but its Offset is not confirmed until Publish has
// succeeded and Pending is cleared. On startup, Pending is retried using the
// event IDs; receivers are responsible for idempotent application because the
// local checkpoint and receiver write are separate systems.
type StateStore struct {
	path string
}

func NewStateStore(path string) *StateStore {
	return &StateStore{path: path}
}

func (s *StateStore) Load() (map[string]Checkpoint, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return map[string]Checkpoint{}, nil
	}
	if err != nil {
		return nil, err
	}
	var states map[string]Checkpoint
	if err := json.Unmarshal(data, &states); err != nil {
		return nil, fmt.Errorf("load checkpoints: %w", err)
	}
	if states == nil {
		states = map[string]Checkpoint{}
	}
	return states, nil
}

func (s *StateStore) Save(states map[string]Checkpoint) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
