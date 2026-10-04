package logengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Gap is a range of a stream that a consumer can no longer obtain. To is the
// exclusive end offset, or -1 when the end is unknown (the file holding the
// range is gone). Reason explains why the range is unavailable:
// "rotated_before_confirm", "skipped_on_join", "abandoned: <reason>" or
// "source_unreadable".
type Gap struct {
	Stream string `json:"stream"`
	FileID FileID `json:"file_id"`
	From   int64  `json:"from"`
	To     int64  `json:"to"`
	Reason string `json:"reason"`
}

// Abandonment records that a consumer deliberately gave up a historical
// range to release retention pressure. It is an explicit audit entry with
// the range and the operator-supplied reason, never a confirmation: the
// records in the range were NOT processed and must not be counted as such.
type Abandonment struct {
	Consumer string `json:"consumer"`
	Stream   string `json:"stream"`
	FileID   FileID `json:"file_id"`
	From     int64  `json:"from"`
	// To is the exclusive end offset, or -1 when the file holding the range
	// no longer exists and its size is unknown.
	To     int64     `json:"to"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// AbandonStore persists abandonment audit entries next to the checkpoint
// file ("<checkpoint>.abandoned.json") so the record of what was given up
// survives restarts and is visible to every engine of the consumer.
type AbandonStore struct {
	path string
	mu   sync.Mutex
}

var (
	abandonStoresMu sync.Mutex
	abandonStores   = map[string]*AbandonStore{}
)

func NewAbandonStore(checkpointPath string) *AbandonStore {
	path := checkpointPath + ".abandoned.json"
	abandonStoresMu.Lock()
	defer abandonStoresMu.Unlock()
	if store, ok := abandonStores[path]; ok {
		return store
	}
	store := &AbandonStore{path: path}
	abandonStores[path] = store
	return store
}

func (s *AbandonStore) load() ([]Abandonment, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entries []Abandonment
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("load abandonments: %w", err)
	}
	return entries, nil
}

// For returns the abandonment entries of one consumer on one stream.
func (s *AbandonStore) For(consumer, stream string) []Abandonment {
	entries, err := s.load()
	if err != nil {
		return nil
	}
	var out []Abandonment
	for _, entry := range entries {
		if entry.Consumer == consumer && entry.Stream == stream {
			out = append(out, entry)
		}
	}
	return out
}

func (s *AbandonStore) append(entry Abandonment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load()
	if err != nil {
		return err
	}
	entries = append(entries, entry)
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Abandonments lists every range this consumer has given up on this stream.
func (e *Engine) Abandonments() []Abandonment {
	return e.abandoned.For(e.consumer, e.stream)
}

// Abandon gives up the unconfirmed historical range of this stream and
// advances the consumer's checkpoint past it WITHOUT publishing anything.
// It exists so a long-paused consumer can release retention pressure. The
// abandonment is persisted as an audit entry carrying the exact range and
// the mandatory reason; it never appears as a confirmed delivery, so the
// skipped records can never be mistaken for successfully processed ones.
//
// Pending undelivered events inside the abandoned range are dropped. The
// returned Abandonment is the persisted audit entry.
func (e *Engine) Abandon(reason string) (Abandonment, error) {
	if reason == "" {
		return Abandonment{}, errors.New("abandon requires a reason")
	}
	key := e.stateKey()
	cp := e.states[key]
	entry := Abandonment{
		Consumer: e.consumer,
		Stream:   e.stream,
		FileID:   cp.FileID,
		From:     cp.Offset,
		To:       -1,
		Reason:   reason,
		At:       time.Now().UTC(),
	}
	id, info, err := fileIDFromPath(e.path)
	switch {
	case err == nil && (cp.FileID.Inode == 0 || cp.FileID == id):
		// The confirmed file is still the current one: the abandoned range
		// ends at the current end of the file.
		entry.FileID = id
		entry.To = info.Size()
	case err == nil:
		// The confirmed file was rotated away; its size is unknown.
		entry.To = -1
	default:
		// The source is gone entirely; only the confirmed offset is known.
		entry.To = -1
	}
	if entry.To >= 0 && entry.To <= entry.From {
		return Abandonment{}, fmt.Errorf("nothing to abandon: confirmed offset %d already covers the available range", entry.From)
	}
	if err := e.abandoned.append(entry); err != nil {
		return Abandonment{}, err
	}

	// Advance the checkpoint past the abandoned range. This is a checkpoint
	// write like any other confirmation, but no Publish happened and the
	// audit entry above is the durable record of why the offset moved.
	next := Checkpoint{}
	if err == nil {
		next.FileID = id
		next.Offset = info.Size()
	}
	e.states[key] = next
	e.fresh = false
	if err := e.store.SaveStream(key, next); err != nil {
		return Abandonment{}, err
	}

	// Drop any pending undelivered events and reset in-process read state so
	// the next Pump starts cleanly at the new checkpoint.
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	e.parser.Reset()
	e.pending = e.pending[:0]
	e.delivered = false
	e.id = FileID{}
	e.offset, e.safeOffset = 0, 0
	e.nextLine = 0
	e.carry = e.carry[:0]
	e.carryStart = 0
	e.resetCommit = false
	return entry, nil
}

// Group pumps several sources independently: one failing or unreadable
// source never blocks the others, and every failure is reported as a
// per-source status with the affected gap.
type Group struct {
	sources []groupSource
}

type groupSource struct {
	engine *Engine
	output Output
}

// SourceStatus reports the outcome of one PumpAll round for one source.
type SourceStatus struct {
	Stream   string
	Consumer string
	// Err is set when the source could not be pumped (for example the file
	// is unreadable); other sources are unaffected.
	Err error
	// Gaps lists the ranges of this source currently unobtainable by the
	// consumer, including the gap caused by Err.
	Gaps []Gap
}

func NewGroup() *Group { return &Group{} }

// Add registers an engine and its output as one independent source.
func (g *Group) Add(engine *Engine, output Output) {
	g.sources = append(g.sources, groupSource{engine: engine, output: output})
}

// PumpAll pumps every registered source once and returns one status per
// source, in registration order. A source that fails keeps its own error and
// gap; the remaining sources are still pumped and confirmed normally.
func (g *Group) PumpAll() []SourceStatus {
	statuses := make([]SourceStatus, 0, len(g.sources))
	for _, source := range g.sources {
		status := SourceStatus{
			Stream:   source.engine.stream,
			Consumer: source.engine.consumer,
		}
		if err := source.engine.Pump(source.output); err != nil {
			status.Err = err
			status.Gaps = append(status.Gaps, Gap{
				Stream: source.engine.stream,
				From:   source.engine.states[source.engine.stateKey()].Offset,
				To:     -1,
				Reason: "source_unreadable",
			})
		}
		status.Gaps = append(status.Gaps, source.engine.RecoveryStatus().Gaps...)
		statuses = append(statuses, status)
	}
	return statuses
}
