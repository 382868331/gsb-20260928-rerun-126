package logengine

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// Two engines tracking different files must share one checkpoint file without
// clobbering each other's confirmed positions.
func TestMultipleStreamsShareCheckpointFile(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "state.json")
	pathA := filepath.Join(dir, "a.log")
	pathB := filepath.Join(dir, "b.log")
	if err := os.WriteFile(pathA, []byte("a1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathB, []byte("b1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	outA := &collectOutput{failAt: -1}
	outB := &collectOutput{failAt: -1}
	engineA, err := NewEngine("a", pathA, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	engineB, err := NewEngine("b", pathB, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := engineA.Pump(outA); err != nil {
		t.Fatal(err)
	}
	if err := engineB.Pump(outB); err != nil {
		t.Fatal(err)
	}
	// A second pump of A must not roll back B's checkpoint.
	if err := engineA.Pump(outA); err != nil {
		t.Fatal(err)
	}
	states, err := NewStateStore(checkpointPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if states["a"].Offset != 3 || states["b"].Offset != 3 {
		t.Fatalf("streams clobbered each other: %+v", states)
	}

	// After a restart each stream resumes at its own confirmed offset.
	if err := os.WriteFile(pathA, []byte("a1\na2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathB, []byte("b1\nb2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reA, err := NewEngine("a", pathA, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	reB, err := NewEngine("b", pathB, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	outA2 := &collectOutput{failAt: -1}
	outB2 := &collectOutput{failAt: -1}
	if err := reA.Pump(outA2); err != nil {
		t.Fatal(err)
	}
	if err := reB.Pump(outB2); err != nil {
		t.Fatal(err)
	}
	gotA := records(t, outA2.events)
	gotB := records(t, outB2.events)
	if len(gotA) != 1 || string(gotA[0].Raw) != "a2" {
		t.Fatalf("stream a resumed wrong: %#v", gotA)
	}
	if len(gotB) != 1 || string(gotB[0].Raw) != "b2" {
		t.Fatalf("stream b resumed wrong: %#v", gotB)
	}
}

// Rotation while collection is paused must finalize the old file's incomplete
// multiline record against the old file identity, not drop it and not join it
// to the head of the new file.
func TestRotationDuringPauseFlushesOldPartialRecord(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	// Two complete multiline records: the first is confirmed, the second is
	// still pending in the parser when the process stops, so the checkpoint
	// carries parser state for the old file.
	if err := os.WriteFile(logPath, []byte("start one\ncont\nstart two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	parser := NewMultilineParser(RegexpStart(regexp.MustCompile(`^start `)), 100, nil)
	// The second publish (the EOF flush of "start two") fails, leaving the
	// checkpoint at the confirmed first record with "start two" as pending
	// parser state.
	out := &collectOutput{failAt: 1}
	e, err := NewEngine("app", logPath, checkpointPath, parser)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	// The second pump reaches EOF and flushes the pending record; that
	// publish fails, leaving parser state in the checkpoint.
	if err := e.Pump(out); err == nil {
		t.Fatal("expected publish failure")
	}
	if got := records(t, out.events); len(got) != 1 || string(got[0].Raw) != "start one\ncont" {
		t.Fatalf("confirmed prefix wrong: %#v", out.events)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Pause window: the file is renamed away and the path is recreated.
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("start three\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	parser2 := NewMultilineParser(RegexpStart(regexp.MustCompile(`^start `)), 100, nil)
	e2, err := NewEngine("app", logPath, checkpointPath, parser2)
	if err != nil {
		t.Fatal(err)
	}
	out2 := &collectOutput{failAt: -1}
	if err := e2.Pump(out2); err != nil {
		t.Fatal(err)
	}
	got := records(t, out2.events)
	if len(got) != 2 {
		t.Fatalf("expected old partial and new head as separate records: %#v", got)
	}
	if string(got[0].Raw) != "start two" {
		t.Fatalf("old partial record lost or corrupted: %q", got[0].Raw)
	}
	if string(got[1].Raw) != "start three" {
		t.Fatalf("new file head wrong: %q", got[1].Raw)
	}
	if got[0].Position.FileID == got[1].Position.FileID {
		t.Fatal("old and new sources share an identity")
	}
	if got[0].ID == got[1].ID {
		t.Fatal("old and new records share a stable ID")
	}
	if got[1].Position.Start != 0 {
		t.Fatalf("new file not read from zero: %+v", got[1].Position)
	}
}

// An unreadable source must fail on its own engine only, and once readable
// again it resumes from the confirmed offset without skipping content.
func TestUnreadableSourceDoesNotBlockOthersAndResumes(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "state.json")
	blockedPath := filepath.Join(dir, "blocked.log")
	okPath := filepath.Join(dir, "ok.log")
	if err := os.WriteFile(blockedPath, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(okPath, []byte("ok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	blockedOut := &collectOutput{failAt: -1}
	okOut := &collectOutput{failAt: -1}
	blocked, err := NewEngine("blocked", blockedPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	ok, err := NewEngine("ok", okPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := blocked.Pump(blockedOut); err != nil {
		t.Fatal(err)
	}

	// The source becomes unreadable while more content is appended.
	f, err := os.OpenFile(blockedPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("second\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chmod(blockedPath, 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(blockedPath, 0o600)

	if err := blocked.Pump(blockedOut); err == nil {
		t.Fatal("expected error for unreadable source")
	}
	if err := ok.Pump(okOut); err != nil {
		t.Fatalf("healthy source blocked by unreadable one: %v", err)
	}
	if got := records(t, okOut.events); len(got) != 1 || string(got[0].Raw) != "ok" {
		t.Fatalf("healthy source wrong: %#v", got)
	}

	// After recovery nothing is skipped and nothing confirmed is repeated.
	if err := os.Chmod(blockedPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := blocked.Pump(blockedOut); err != nil {
		t.Fatal(err)
	}
	got := records(t, blockedOut.events)
	if len(got) != 2 || string(got[0].Raw) != "first" || string(got[1].Raw) != "second" {
		t.Fatalf("recovery skipped or duplicated content: %#v", got)
	}
}

// RecoveryStatus reconciles the checkpoint with the receiver's confirmations.
func TestRecoveryStatusReportsConfirmedAndReplayRange(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := &collectOutput{failAt: -1}
	e, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Pump(out); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash window: delivered but the checkpoint is lost.
	if err := os.Remove(checkpointPath); err != nil {
		t.Fatal(err)
	}
	e2, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	status := e2.RecoveryStatus()
	if status.Confirmed.Offset != 0 {
		t.Fatalf("lost checkpoint should confirm nothing: %+v", status.Confirmed)
	}
	if !status.SameFile || status.UnconfirmedBytes != 2 {
		t.Fatalf("replay range wrong: %+v", status)
	}

	// After a confirmed pump the whole file is acknowledged.
	out2 := &collectOutput{failAt: -1}
	if err := e2.Pump(out2); err != nil {
		t.Fatal(err)
	}
	status = e2.RecoveryStatus()
	if status.Confirmed.Offset != 2 || status.UnconfirmedBytes != 0 {
		t.Fatalf("confirmed status wrong: %+v", status)
	}

	// Rotation during a pause reports a fresh, fully unconfirmed file.
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e3, err := NewEngine("app", logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil))
	if err != nil {
		t.Fatal(err)
	}
	status = e3.RecoveryStatus()
	if status.SameFile {
		t.Fatal("rotated file reported as the confirmed file")
	}
	if status.UnconfirmedBytes != 4 {
		t.Fatalf("new file should be fully unconfirmed: %+v", status)
	}
}
