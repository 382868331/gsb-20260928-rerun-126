package logengine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// countingOutput records how often each ID was published and dedupes like a
// real receiver must: one ID becomes at most one business record.
type countingOutput struct {
	published map[string]int
	order     []Event
	fail      bool
}

func newCountingOutput() *countingOutput {
	return &countingOutput{published: map[string]int{}}
}

func (c *countingOutput) Publish(events []Event) error {
	if c.fail {
		return errors.New("output unavailable")
	}
	c.order = append(c.order, events...)
	for _, event := range events {
		if record, ok := event.(Record); ok {
			c.published[record.ID]++
		}
	}
	return nil
}

func (c *countingOutput) calls() int {
	n := 0
	for _, v := range c.published {
		n += v
	}
	return n
}

func sabotageCheckpointDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func restoreCheckpointDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointFailureAfterPublishRetriesSaveOnly(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	cpDir := filepath.Join(dir, "state")
	checkpointPath := filepath.Join(cpDir, "state.json")
	if err := os.MkdirAll(cpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	out := newCountingOutput()

	// Publish succeeds, then the checkpoint write fails.
	sabotageCheckpointDir(t, cpDir)
	if err := e.Pump(out); !errors.Is(err, ErrCheckpointNotSaved) {
		t.Fatalf("expected ErrCheckpointNotSaved, got %v", err)
	}
	if out.calls() != 1 {
		t.Fatalf("record should be published exactly once, got %d", out.calls())
	}

	// Retrying while storage is still broken must not publish again.
	if err := e.Pump(out); !errors.Is(err, ErrCheckpointNotSaved) {
		t.Fatalf("retry should keep failing to save, got %v", err)
	}
	if out.calls() != 1 {
		t.Fatalf("failed checkpoint retry republished the record: %d", out.calls())
	}

	// Once storage recovers the checkpoint saves without a second publish and
	// reading continues with the next record.
	restoreCheckpointDir(t, cpDir)
	if err := e.Pump(out); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatalf("read next record: %v", err)
	}
	if out.calls() != 2 {
		t.Fatalf("expected two published records, got %d", out.calls())
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLostCheckpointReplaysSameStableID(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// First process: delivers "a" and confirms it at the receiver.
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	first := newCountingOutput()
	if err := e.Pump(first); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if first.calls() != 1 {
		t.Fatalf("expected one delivery, got %d", first.calls())
	}
	firstID := first.order[0].(Record).ID

	// Simulate a crash between Publish and checkpoint persistence: the
	// checkpoint file is gone, the receiver already holds the record.
	if err := os.Remove(checkpointPath); err != nil {
		t.Fatal(err)
	}
	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	replay := newCountingOutput()
	if err := e2.Pump(replay); err != nil {
		t.Fatal(err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	if replay.calls() != 1 {
		t.Fatalf("expected one replay delivery, got %d", replay.calls())
	}
	replayedID := replay.order[0].(Record).ID
	if replayedID != firstID {
		t.Fatalf("redelivery changed record identity: %q != %q", replayedID, firstID)
	}

	// Receiver-side dedup on the stable ID yields a single business record.
	dedup := newCountingOutput()
	_ = dedup.Publish(first.order)
	_ = dedup.Publish(replay.order)
	if len(dedup.published) != 1 {
		t.Fatalf("receiver dedup created duplicate business records: %v", dedup.published)
	}
}
