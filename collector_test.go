package logengine

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func multilineStartParser() *MultilineParser {
	parser := NewMultilineParser(RegexpStart(regexp.MustCompile(`^(old|new)-start`)), 100, nil)
	WithMultilineRetainAtEOF(true)(parser)
	return parser
}

func TestPausedRotationFinishesRenamedOldFileBeforeNewFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("old-start\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := newCountingOutput()
	e, err := NewEngine("app", logPath, checkpointPath, multilineStartParser(), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	rotated := logPath + ".1"
	if err := os.Rename(logPath, rotated); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rotated, []byte("old-start\nold-cont"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("new-start\nnew-cont\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine("app", logPath, checkpointPath, multilineStartParser(), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	for range 80 {
		if err := e2.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	got := recordsFromEvents(out.order)
	if len(got) != 2 || string(got[0].Raw) != "old-start\nold-cont" || string(got[1].Raw) != "new-start\nnew-cont" {
		t.Fatalf("old and new files were not kept separate: %#v", got)
	}
	if got[0].Position.FileID == got[1].Position.FileID {
		t.Fatalf("rotated source identities collided: %+v %+v", got[0].Position.FileID, got[1].Position.FileID)
	}
	if got[1].Position.Start != 0 {
		t.Fatalf("new file did not start at zero: %+v", got[1].Position)
	}
}

func TestPausedTruncateRenameAndRecreateDoesNotSkipReplacement(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := newCountingOutput()
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	rotated := logPath + ".1"
	if err := os.Rename(logPath, rotated); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rotated, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e2.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	bads := bad(out.order)
	if len(bads) != 1 || bads[0].Reason != "file_truncated" {
		t.Fatalf("expected one truncation marker: %#v", out.order)
	}
	got := recordsFromEvents(out.order)
	if len(got) != 2 || string(got[1].Raw) != "new" {
		t.Fatalf("replacement content was skipped: %#v", got)
	}
}

func TestCollectorContinuesReadableSourceWhenOneSourceIsUnreadable(t *testing.T) {
	dir := t.TempDir()
	goodPath := filepath.Join(dir, "good.log")
	blocker := filepath.Join(dir, "blocker")
	badPath := filepath.Join(blocker, "bad.log")
	if err := os.WriteFile(goodPath, []byte("good\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewCollector(filepath.Join(dir, "state.json"), []Source{
		{Stream: "bad", Path: badPath, Parser: NewMultilineParser(AlwaysStart, 100, nil)},
		{Stream: "good", Path: goodPath, Parser: NewMultilineParser(AlwaysStart, 100, nil)},
	}, WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	out := newCountingOutput()
	err = c.Pump(out)
	if err == nil {
		t.Fatal("expected bad source error")
	}
	var sourceErr SourceError
	if !errors.As(err, &sourceErr) || sourceErr.Source.Stream != "bad" {
		t.Fatalf("error was not attributed to bad source: %v", err)
	}
	got := recordsFromEvents(out.order)
	if len(got) != 1 || string(got[0].Raw) != "good" {
		t.Fatalf("readable source was blocked: %#v", got)
	}
	reports := c.Recovery()
	if len(reports) != 2 || reports[0].Readable || reports[1].Status == RecoveryUnavailable {
		t.Fatalf("unexpected recovery reports: %#v", reports)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRotationBoundaryCheckpointFailureRetriesSaveOnly(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	cpDir := filepath.Join(dir, "state")
	checkpointPath := filepath.Join(cpDir, "state.json")
	if err := os.MkdirAll(cpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("old-start\nold-cont"), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine("app", logPath, checkpointPath, multilineStartParser(), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(newCountingOutput()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := newCountingOutput()
	sabotageCheckpointDir(t, cpDir)
	if err := e.Pump(out); !errors.Is(err, ErrCheckpointNotSaved) {
		t.Fatalf("expected checkpoint failure, got %v", err)
	}
	if got := recordsFromEvents(out.order); len(got) != 1 || string(got[0].Raw) != "old-start\nold-cont" {
		t.Fatalf("old boundary was not published once: %#v", got)
	}
	if err := e.Pump(out); !errors.Is(err, ErrCheckpointNotSaved) {
		t.Fatalf("retry should only save, got %v", err)
	}
	if len(out.order) != 1 {
		t.Fatalf("failed boundary save republished events: %#v", out.order)
	}
	restoreCheckpointDir(t, cpDir)
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	got := recordsFromEvents(out.order)
	if len(got) != 2 || string(got[1].Raw) != "new" {
		t.Fatalf("new file was skipped after checkpoint recovery: %#v", got)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryReportSeparatesConfirmedAndPossibleDuplicate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	out := newCountingOutput()
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithReadSize(4))
	if err != nil {
		t.Fatal(err)
	}
	report := e2.Recovery()
	if report.Status != RecoveryPossibleDuplicate {
		t.Fatalf("expected possible duplicate, got %#v", report)
	}
	if report.ConfirmedOffset != 2 || report.LastRecordID == "" || report.PossibleDuplicate == nil {
		t.Fatalf("confirmed and uncertain ranges were not separated: %#v", report)
	}
	if report.PossibleDuplicate.Start != 2 || report.PossibleDuplicate.End != 4 {
		t.Fatalf("unexpected possible duplicate range: %#v", report.PossibleDuplicate)
	}
}

func recordsFromEvents(events []Event) []Record {
	var records []Record
	for _, event := range events {
		if record, ok := event.(Record); ok {
			records = append(records, record)
		}
	}
	return records
}
