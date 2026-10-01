package logengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Checkpoint struct {
	Path                string          `json:"path,omitempty"`
	FileID              FileID          `json:"file_id"`
	LastConfirmedFileID FileID          `json:"last_confirmed_file_id,omitempty"`
	Size                int64           `json:"size,omitempty"`
	Offset              int64           `json:"offset"`
	NextLine            int             `json:"next_line"`
	LastRecordID        string          `json:"last_record_id,omitempty"`
	ParserState         json.RawMessage `json:"parser_state,omitempty"`
}

// StateStore stores the highest fully confirmed byte offset for each stream.
//
// A checkpoint only records what was already confirmed by the Output; saving
// it is a separate write from publishing and the two are never atomic. Save
// uses a temp-file rename so a crash during the write leaves the previous
// checkpoint intact. Recovery after a failed save redelivers the tail, which
// is why Outputs must deduplicate on Record.ID.
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
