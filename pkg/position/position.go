// Package position persists read positions of log sources so that an
// engine restart resumes after the last acknowledged record.
package position

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Cursor is the committed read position for a single source path.
//
// Dev/Inode identify the file on Unix-like systems. HeadFingerprint is a
// digest of the first bytes of the file and guards against inode reuse:
// a rotated file that happens to reuse an inode must not be resumed at an
// offset that belongs to unrelated content.
type Cursor struct {
	Path            string `json:"path"`
	Dev             uint64 `json:"dev"`
	Inode           uint64 `json:"inode"`
	HeadFingerprint string `json:"head_fingerprint"`
	HeadLen         int64  `json:"head_len"`
	Offset          int64  `json:"offset"`
}

// Store is an atomic, mutex protected JSON cursor store.
type Store struct {
	mu      sync.Mutex
	path    string
	cursors map[string]Cursor
}

// Open loads (or creates) a cursor store at path.
func Open(path string) (*Store, error) {
	s := &Store{path: path, cursors: map[string]Cursor{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &s.cursors); err != nil {
			return nil, fmt.Errorf("load position store %s: %w", path, err)
		}
	}
	return s, nil
}

// Get returns the committed cursor for path.
func (s *Store) Get(path string) (Cursor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cursors[path]
	return c, ok
}

// Commit stores c and durably flushes the whole store. Only offsets that
// have been acknowledged by the output must be committed.
func (s *Store) Commit(c Cursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[c.Path] = c
	return s.flushLocked()
}

func (s *Store) flushLocked() error {
	data, err := json.MarshalIndent(s.cursors, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".position-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}
