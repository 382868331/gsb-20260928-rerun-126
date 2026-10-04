package logengine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// JoinPolicy selects where a consumer with no prior progress starts reading.
type JoinPolicy int

const (
	// JoinOldest starts at the beginning of the current file.
	JoinOldest JoinPolicy = iota
	// JoinNewest starts at the current end of the file, skipping existing
	// content by choice. Content already rotated or truncated away is not
	// skipped by choice; it is reported through JoinReport.Unavailable.
	JoinNewest
)

// Gap is a byte range of a stream that can no longer be read from the source
// path, for example because the file was rotated away or truncated while the
// consumer was paused. To is -1 when the lost range runs to the (unknown) end
// of the replaced file.
type Gap struct {
	Consumer string    `json:"consumer"`
	Stream   string    `json:"stream"`
	FileID   FileID    `json:"file_id"`
	From     int64     `json:"from"`
	To       int64     `json:"to"`
	Reason   string    `json:"reason"`
	At       time.Time `json:"at"`
}

// Abandonment records that a consumer deliberately gave up a byte range to
// relieve retention pressure. It is kept separate from the confirmed
// checkpoint so a give-up is never indistinguishable from processed work.
type Abandonment struct {
	Stream string    `json:"stream"`
	FileID FileID    `json:"file_id"`
	From   int64     `json:"from"`
	To     int64     `json:"to"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// ConsumerState is the persistent identity of one consumer: its own confirmed
// checkpoint per stream plus the ranges it abandoned. One consumer's
// confirmations never appear in another consumer's state.
type ConsumerState struct {
	Checkpoints map[string]Checkpoint `json:"checkpoints"`
	Abandoned   []Abandonment         `json:"abandoned,omitempty"`
}

type consumerFileState struct {
	Consumers map[string]*ConsumerState `json:"consumers"`
	Gaps      map[string][]Gap          `json:"gaps,omitempty"`
}

// ConsumerStore persists independent consumer identities and their per-stream
// progress in one file. Writes use temp-file rename like StateStore, and all
// mutations merge under a lock so consumers never clobber each other.
type ConsumerStore struct {
	path string
	mu   sync.Mutex
}

var (
	sharedConsumerStoresMu sync.Mutex
	sharedConsumerStores   = map[string]*ConsumerStore{}
)

func NewConsumerStore(path string) *ConsumerStore {
	sharedConsumerStoresMu.Lock()
	defer sharedConsumerStoresMu.Unlock()
	if store, ok := sharedConsumerStores[path]; ok {
		return store
	}
	store := &ConsumerStore{path: path}
	sharedConsumerStores[path] = store
	return store
}

func (s *ConsumerStore) load() (consumerFileState, error) {
	state := consumerFileState{
		Consumers: map[string]*ConsumerState{},
		Gaps:      map[string][]Gap{},
	}
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := jsonUnmarshal(data, &state); err != nil {
		return state, fmt.Errorf("load consumers: %w", err)
	}
	if state.Consumers == nil {
		state.Consumers = map[string]*ConsumerState{}
	}
	if state.Gaps == nil {
		state.Gaps = map[string][]Gap{}
	}
	for _, c := range state.Consumers {
		if c.Checkpoints == nil {
			c.Checkpoints = map[string]Checkpoint{}
		}
	}
	return state, nil
}

func (s *ConsumerStore) save(state consumerFileState) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := jsonMarshal(state)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// JoinReport describes where a consumer starts and what is already gone.
type JoinReport struct {
	Consumer string
	Stream   string
	// Rejoined is true when the consumer identity already existed and its
	// persisted checkpoint was resumed instead of applying the join policy.
	Rejoined bool
	// Start is the checkpoint the consumer will read from.
	Start Checkpoint
	// Unavailable lists ranges of this stream that can no longer be read
	// from the source at all (rotated away or truncated), independent of
	// the chosen join policy.
	Unavailable []Gap
}

// Join registers a persistent consumer identity for a stream, or resumes an
// existing one. A rejoining consumer keeps its saved checkpoint; the policy
// only applies to a consumer/stream pair seen for the first time.
func (s *ConsumerStore) Join(consumer, stream, path string, policy JoinPolicy) (JoinReport, error) {
	if consumer == "" {
		return JoinReport{}, errors.New("consumer name must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return JoinReport{}, err
	}
	cs, ok := state.Consumers[consumer]
	if !ok {
		cs = &ConsumerState{Checkpoints: map[string]Checkpoint{}}
		state.Consumers[consumer] = cs
	}
	report := JoinReport{Consumer: consumer, Stream: stream, Unavailable: state.Gaps[stream]}
	if cp, ok := cs.Checkpoints[stream]; ok {
		report.Rejoined = true
		report.Start = cp
		return report, s.save(state)
	}
	cp := Checkpoint{}
	if policy == JoinNewest {
		id, info, statErr := fileIDFromPath(path)
		if statErr != nil && !os.IsNotExist(statErr) {
			return JoinReport{}, statErr
		}
		if statErr == nil {
			cp.FileID = id
			cp.Offset = info.Size()
		}
	}
	cs.Checkpoints[stream] = cp
	report.Start = cp
	return report, s.save(state)
}

// SaveStream persists one consumer's confirmed checkpoint for one stream. It
// ensureConsumer makes sure the identity exists so an engine can attach to it
// without an explicit Join; the implicit starting point is JoinOldest.
func (s *ConsumerStore) ensureConsumer(consumer string) (*ConsumerState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return nil, err
	}
	cs, ok := state.Consumers[consumer]
	if !ok {
		cs = &ConsumerState{Checkpoints: map[string]Checkpoint{}}
		state.Consumers[consumer] = cs
		if err := s.save(state); err != nil {
			return nil, err
		}
	}
	return cs, nil
}

// SaveStream persists one consumer's confirmed checkpoint for one stream. It
// merges into the on-disk state under a lock, so consumer A's save never
// rolls back consumer B's progress.
func (s *ConsumerStore) SaveStream(consumer, stream string, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return err
	}
	cs, ok := state.Consumers[consumer]
	if !ok {
		cs = &ConsumerState{Checkpoints: map[string]Checkpoint{}}
		state.Consumers[consumer] = cs
	}
	cs.Checkpoints[stream] = cp
	return s.save(state)
}

// Checkpoints returns a copy of one consumer's per-stream checkpoints.
func (s *ConsumerStore) Checkpoints(consumer string) (map[string]Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return nil, err
	}
	out := map[string]Checkpoint{}
	if cs, ok := state.Consumers[consumer]; ok {
		for stream, cp := range cs.Checkpoints {
			out[stream] = cp
		}
	}
	return out, nil
}

// Abandonments returns the ranges a consumer deliberately gave up.
func (s *ConsumerStore) Abandonments(consumer string) ([]Abandonment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return nil, err
	}
	if cs, ok := state.Consumers[consumer]; ok {
		return append([]Abandonment(nil), cs.Abandoned...), nil
	}
	return nil, nil
}

// Gaps returns the unavailable ranges recorded for a stream.
func (s *ConsumerStore) Gaps(stream string) ([]Gap, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return nil, err
	}
	return append([]Gap(nil), state.Gaps[stream]...), nil
}

func (s *ConsumerStore) abandon(consumer string, ab Abandonment, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return err
	}
	cs, ok := state.Consumers[consumer]
	if !ok {
		return fmt.Errorf("consumer %q has not joined", consumer)
	}
	cs.Abandoned = append(cs.Abandoned, ab)
	cs.Checkpoints[ab.Stream] = cp
	return s.save(state)
}

func (s *ConsumerStore) noteGap(gap Gap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return err
	}
	for _, existing := range state.Gaps[gap.Stream] {
		if existing.Consumer == gap.Consumer && existing.FileID == gap.FileID &&
			existing.From == gap.From && existing.To == gap.To && existing.Reason == gap.Reason {
			return nil
		}
	}
	state.Gaps[gap.Stream] = append(state.Gaps[gap.Stream], gap)
	return s.save(state)
}
