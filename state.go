package logengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Checkpoint struct {
	FileID      FileID          `json:"file_id"`
	Offset      int64           `json:"offset"`
	NextLine    int             `json:"next_line"`
	ParserState json.RawMessage `json:"parser_state,omitempty"`
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
	mu   sync.Mutex
}

// sharedStores hands the same *StateStore to every engine using one checkpoint
// path, so concurrent streams tracked in a single process never overwrite each
// other's confirmed positions.
var (
	sharedStoresMu sync.Mutex
	sharedStores   = map[string]*StateStore{}
)

func NewStateStore(path string) *StateStore {
	sharedStoresMu.Lock()
	defer sharedStoresMu.Unlock()
	if store, ok := sharedStores[path]; ok {
		return store
	}
	store := &StateStore{path: path}
	sharedStores[path] = store
	return store
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

// SaveStream persists the confirmed checkpoint for one stream. It merges into
// the on-disk state under a lock instead of rewriting a stale in-memory map,
// so saving stream A never rolls back the checkpoint of stream B.
func (s *StateStore) SaveStream(stream string, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	states, err := s.Load()
	if err != nil {
		return err
	}
	states[stream] = cp
	return s.save(states)
}

func (s *StateStore) save(states map[string]Checkpoint) error {
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
