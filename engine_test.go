package logengine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type collectOutput struct {
	events []Event
	failAt int
}

func (c *collectOutput) Publish(events []Event) error {
	if c.failAt >= 0 && len(c.events) >= c.failAt {
		return errors.New("output unavailable")
	}
	c.events = append(c.events, events...)
	return nil
}

type failingAckStore struct {
	path     string
	failSave bool
}

func (s *failingAckStore) Load() (map[string]Checkpoint, error) {
	return NewStateStore(s.path).Load()
}

func (s *failingAckStore) Save(states map[string]Checkpoint) error {
	if s.failSave && pendingHasCommittedPosition(states) {
		return errors.New("checkpoint unavailable")
	}
	return NewStateStore(s.path).Save(states)
}

func pendingHasCommittedPosition(states map[string]Checkpoint) bool {
	for _, cp := range states {
		return cp.Pending == nil && cp.Offset > 0
	}
	return false
}

func loadCheckpoint(t *testing.T, path string) Checkpoint {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var states map[string]Checkpoint
	if err := json.Unmarshal(data, &states); err != nil {
		t.Fatal(err)
	}
	return states["app"]
}

func records(t *testing.T, events []Event) []Record {
	t.Helper()
	var out []Record
	for _, event := range events {
		switch v := event.(type) {
		case Record:
			out = append(out, v)
		case BadRecord:
			t.Fatalf("unexpected bad record: %+v", v)
		}
	}
	return out
}

func bad(events []Event) []BadRecord {
	var out []BadRecord
	for _, event := range events {
		if v, ok := event.(BadRecord); ok {
			out = append(out, v)
		}
	}
	return out
}

func TestMultilineIncrementalReadAndRestartResume(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("start one\ncontinuation\nstart two"), 0o600); err != nil {
		t.Fatal(err)
	}
	parser := NewMultilineParser(RegexpStart(regexp.MustCompile(`^start `)), 100, nil)
	out := &collectOutput{failAt: -1}

	e, err := NewEngine("app", logPath, checkpointPath, parser, WithReadSize(1), WithMaxLineBytes(100))
	if err != nil {
		t.Fatal(err)
	}
	for range 30 {
		if err := e.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
	if got := records(t, out.events); len(got) != 1 {
		t.Fatalf("unexpected records: %#v", got)
	}
	if _, err := os.Stat(checkpointPath); err != nil {
		t.Fatalf("checkpoint not saved: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(logPath, []byte("start one\ncontinuation\nstart two\nstart three"), 0o600); err != nil {
		t.Fatal(err)
	}
	parser2 := NewMultilineParser(RegexpStart(regexp.MustCompile(`^start `)), 100, nil)
	e2, err := NewEngine("app", logPath, checkpointPath, parser2, WithReadSize(1), WithMaxLineBytes(100))
	if err != nil {
		t.Fatal(err)
	}
	for range 80 {
		if err := e2.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
	var got []Record
	for _, event := range out.events {
		if record, ok := event.(Record); ok {
			got = append(got, record)
		}
	}
	if len(got) != 2 || string(got[1].Raw) != "start two" {
		t.Fatalf("resume duplicated or lost records: %#v", got)
	}
	if got[1].Position.Start != 23 || got[1].Position.End != 33 {
		t.Fatalf("resumed record did not use byte position: %+v", got[1].Position)
	}
}

func TestFailedPublishDoesNotAdvanceCheckpoint(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	failing := &collectOutput{failAt: 0}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(failing); err == nil {
		t.Fatal("expected publish failure")
	}
	cp := loadCheckpoint(t, checkpointPath)
	if cp.Offset != 0 || cp.Pending == nil {
		t.Fatalf("publish failure advanced confirmation: %+v", cp)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	success := &collectOutput{failAt: -1}
	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Pump(success); err != nil {
		t.Fatal(err)
	}
	got := records(t, success.events)
	if len(got) != 1 || string(got[0].Raw) != "a" {
		t.Fatalf("unacknowledged record was lost: %#v", got)
	}
}

func TestRotationDoesNotTreatNewFileAsOldTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("old-start\nold-cont\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	parser := NewMultilineParser(RegexpStart(regexp.MustCompile(`^old-start|^new-start`)), 100, nil)
	out := &collectOutput{failAt: -1}
	e, err := NewEngine("app", logPath, checkpointPath, parser, WithReadSize(3))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("new-start\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var got []Record
	for _, event := range out.events {
		if record, ok := event.(Record); ok {
			got = append(got, record)
		}
	}
	if len(got) != 2 || string(got[0].Raw) != "old-start\nold-cont" || string(got[1].Raw) != "new-start" {
		t.Fatalf("rotation records wrong: %#v", got)
	}
	if got[1].Position.Start != 0 {
		t.Fatalf("new file was read as old tail: %+v", got[1].Position)
	}
	if got[0].Position.FileID == got[1].Position.FileID {
		t.Fatalf("rotated files share identity: %+v", got)
	}
}

func TestTruncationReportsPositionAndRestartsAtZero(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := &collectOutput{failAt: -1}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	bads := bad(out.events)
	if len(bads) != 1 || bads[0].Reason != "file_truncated" {
		t.Fatalf("expected truncation bad record, got %#v", out.events)
	}
	if bads[0].Position.Start != 4 {
		t.Fatalf("truncation lost old position: %+v", bads[0].Position)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	var got []Record
	for _, event := range out.events {
		if record, ok := event.(Record); ok {
			got = append(got, record)
		}
	}
	if len(got) != 2 || string(got[1].Raw) != "x" || got[1].Position.Start != 0 {
		t.Fatalf("truncated file not reread from zero: %#v", got)
	}
}

func TestOversizedAndBadJSONDoNotLeakRawContent(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	secret := "token=super-secret-value-0123456789abc"
	if err := os.WriteFile(logPath, []byte("{\"good\":true}\n"+secret+"\n{bad json}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, NewJSONParser()), WithReadSize(5), WithMaxLineBytes(40))
	if err != nil {
		t.Fatal(err)
	}
	out := &collectOutput{failAt: -1}
	for range 10 {
		if err := e.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var valid []Record
	for _, event := range out.events {
		if record, ok := event.(Record); ok {
			valid = append(valid, record)
		}
	}
	if len(valid) != 1 {
		t.Fatalf("expected one valid record: %#v", out.events)
	}
	bads := bad(out.events)
	if len(bads) != 2 {
		t.Fatalf("expected two bad records: %#v", bads)
	}
	for _, b := range bads {
		if strings.Contains(b.Reason, secret) {
			t.Fatalf("reason leaked payload: %q", b.Reason)
		}
	}
}

type idempotentOutput struct {
	records map[EventID]Record
}

func (o *idempotentOutput) Publish(events []Event) error {
	if o.records == nil {
		o.records = make(map[EventID]Record)
	}
	for _, event := range events {
		if record, ok := event.(Record); ok {
			o.records[record.ID] = record
		}
	}
	return nil
}

func TestConfirmedOutputWithFailedCheckpointRetriesById(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &failingAckStore{path: checkpointPath, failSave: true}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2), WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	out := &idempotentOutput{}
	if err := e.Pump(out); err == nil {
		t.Fatal("expected final checkpoint failure")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if len(out.records) != 1 {
		t.Fatalf("expected one confirmed record, got %#v", out.records)
	}
	var firstID EventID
	for id := range out.records {
		firstID = id
	}
	cp := loadCheckpoint(t, checkpointPath)
	if cp.Offset != 0 || cp.Pending == nil || len(cp.Pending.Events) != 1 {
		t.Fatalf("confirmed batch was not retained for recovery: %+v", cp)
	}

	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e2.Pump(out); err != nil {
		t.Fatal(err)
	}
	if len(out.records) != 2 {
		t.Fatalf("retry changed record set: %#v", out.records)
	}
	if _, ok := out.records[firstID]; !ok {
		t.Fatalf("retry changed stable identity: %#v", out.records)
	}
	cp = loadCheckpoint(t, checkpointPath)
	if cp.Pending != nil || cp.Offset != 4 {
		t.Fatalf("retry did not complete read position: %+v", cp)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConfirmedBatchPendingAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &failingAckStore{path: checkpointPath, failSave: true}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2), WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	out := &idempotentOutput{}
	if err := e.Pump(out); err == nil {
		t.Fatal("expected final checkpoint failure")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var oldID EventID
	for id := range out.records {
		oldID = id
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("n\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := e2.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	if len(out.records) != 2 {
		t.Fatalf("rotation retried or dropped records: %#v", out.records)
	}
	if _, ok := out.records[oldID]; !ok {
		t.Fatalf("rotated retry changed old identity: %#v", out.records)
	}
	cp := loadCheckpoint(t, checkpointPath)
	if cp.Pending != nil || cp.Offset != 2 {
		t.Fatalf("rotation read position wrong: %+v", cp)
	}
}

func TestConfirmedBatchPendingAcrossTruncation(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &failingAckStore{path: checkpointPath, failSave: true}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4), WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	out := &idempotentOutput{}
	if err := e.Pump(out); err == nil {
		t.Fatal("expected final checkpoint failure")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var oldID EventID
	for id := range out.records {
		oldID = id
	}
	if err := os.WriteFile(logPath, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := e2.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	if len(out.records) != 2 {
		t.Fatalf("truncation retried or dropped records: %#v", out.records)
	}
	if _, ok := out.records[oldID]; !ok {
		t.Fatalf("truncation retry changed old identity: %#v", out.records)
	}
	if _, ok := out.records[func() EventID {
		for _, record := range out.records {
			if string(record.Raw) == "x" {
				return record.ID
			}
		}
		return ""
	}()]; !ok {
		t.Fatalf("truncated file was not reread: %#v", out.records)
	}
	cp := loadCheckpoint(t, checkpointPath)
	if cp.Pending != nil || cp.Offset != 2 {
		t.Fatalf("truncation read position wrong: %+v", cp)
	}
}
