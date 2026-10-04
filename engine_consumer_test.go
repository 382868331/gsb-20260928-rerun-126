package logengine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newConsumerEngine(t *testing.T, consumer, stream, logPath, checkpointPath string, cs *ConsumerStore) *Engine {
	t.Helper()
	e, err := NewEngine(stream, logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), WithConsumer(consumer, cs))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// newSlowConsumerEngine reads two bytes per pump so a test can stop a
// consumer between records.
func newSlowConsumerEngine(t *testing.T, consumer, stream, logPath, checkpointPath string, cs *ConsumerStore) *Engine {
	t.Helper()
	e, err := NewEngine(stream, logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil),
		WithConsumer(consumer, cs), WithReadSize(2))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// A fast and a slow consumer confirm independently: the fast consumer's
// progress never advances the slow one, both see the same stable record IDs,
// and after a pause and rejoin each resumes from its own checkpoint.
func TestFastSlowConsumersAckIndependently(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	consumerPath := filepath.Join(dir, "consumers.json")
	if err := os.WriteFile(logPath, []byte("a\nb\nc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cs := NewConsumerStore(consumerPath)
	if _, err := cs.Join("fast", "app", logPath, JoinOldest); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Join("slow", "app", logPath, JoinOldest); err != nil {
		t.Fatal(err)
	}

	fast := newConsumerEngine(t, "fast", "app", logPath, checkpointPath, cs)
	fastOut := &collectOutput{failAt: -1}
	for range 5 {
		if err := fast.Pump(fastOut); err != nil {
			t.Fatal(err)
		}
	}

	// The slow consumer confirms only the first record, then its output fails.
	slow := newSlowConsumerEngine(t, "slow", "app", logPath, checkpointPath, cs)
	slowOut := &collectOutput{failAt: 1}
	if err := slow.Pump(slowOut); err != nil {
		t.Fatal(err)
	}
	if err := slow.Pump(slowOut); err == nil {
		t.Fatal("expected slow consumer publish failure")
	}

	cps, err := cs.Checkpoints("fast")
	if err != nil {
		t.Fatal(err)
	}
	if cps["app"].Offset != 6 {
		t.Fatalf("fast consumer should be caught up: %+v", cps["app"])
	}
	slowCps, err := cs.Checkpoints("slow")
	if err != nil {
		t.Fatal(err)
	}
	if slowCps["app"].Offset != 4 {
		t.Fatalf("slow consumer must not inherit fast progress: %+v", slowCps["app"])
	}

	// Both consumers saw the same record with the same stable ID.
	fastFirst := records(t, fastOut.events)[0]
	slowFirst := records(t, slowOut.events)[0]
	if fastFirst.ID != slowFirst.ID {
		t.Fatalf("record identity differs across consumers: %q != %q", fastFirst.ID, slowFirst.ID)
	}

	// The slow consumer pauses; the fast consumer keeps going on new data.
	if err := slow.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("d\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := fast.Pump(fastOut); err != nil {
		t.Fatal(err)
	}

	// Slow exits and rejoins: its own checkpoint is resumed, not fast's.
	report, err := cs.Join("slow", "app", logPath, JoinNewest)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Rejoined {
		t.Fatal("existing consumer identity must rejoin, not re-join")
	}
	if report.Start.Offset != 4 {
		t.Fatalf("rejoin must resume the saved checkpoint, got %+v", report.Start)
	}
	slow2 := newConsumerEngine(t, "slow", "app", logPath, checkpointPath, cs)
	slowOut2 := &collectOutput{failAt: -1}
	for range 5 {
		if err := slow2.Pump(slowOut2); err != nil {
			t.Fatal(err)
		}
	}
	got := records(t, slowOut2.events)
	if len(got) != 3 || string(got[0].Raw) != "b" || string(got[2].Raw) != "d" {
		t.Fatalf("slow consumer resumed wrong: %#v", got)
	}
	if got[0].ID == fastFirst.ID {
		t.Fatal("slow consumer redelivered an already confirmed record")
	}

	// Both consumers end at the same offset by their own paths.
	cps, _ = cs.Checkpoints("fast")
	slowCps, _ = cs.Checkpoints("slow")
	if cps["app"].Offset != 8 || slowCps["app"].Offset != 8 {
		t.Fatalf("final checkpoints wrong: fast=%+v slow=%+v", cps["app"], slowCps["app"])
	}
}

// Consumer progress survives a save/reopen cycle and is persisted on disk
// per consumer identity.
func TestConsumerProgressRestoredAfterReopen(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	consumerPath := filepath.Join(dir, "consumers.json")
	if err := os.WriteFile(logPath, []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cs := NewConsumerStore(consumerPath)
	e1 := newSlowConsumerEngine(t, "alice", "app", logPath, checkpointPath, cs)
	out1 := &collectOutput{failAt: 1}
	var pumpErr error
	for range 8 {
		if pumpErr = e1.Pump(out1); pumpErr != nil {
			break
		}
	}
	if pumpErr == nil {
		t.Fatal("expected publish failure")
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	// The on-disk state names the consumer and its confirmed offset.
	data, err := os.ReadFile(consumerPath)
	if err != nil {
		t.Fatal(err)
	}
	var disk struct {
		Consumers map[string]struct {
			Checkpoints map[string]Checkpoint `json:"checkpoints"`
		} `json:"consumers"`
	}
	if err := json.Unmarshal(data, &disk); err != nil {
		t.Fatal(err)
	}
	if disk.Consumers["alice"].Checkpoints["app"].Offset != 8 {
		t.Fatalf("consumer progress not persisted under its identity: %s", data)
	}

	// Reopen: alice resumes at "two", a different consumer starts from zero.
	e2 := newConsumerEngine(t, "alice", "app", logPath, checkpointPath, cs)
	out2 := &collectOutput{failAt: -1}
	for range 5 {
		if err := e2.Pump(out2); err != nil {
			t.Fatal(err)
		}
	}
	got := records(t, out2.events)
	if len(got) != 1 || string(got[0].Raw) != "two" {
		t.Fatalf("alice did not resume her own progress: %#v", got)
	}
	e3 := newConsumerEngine(t, "bob", "app", logPath, checkpointPath, cs)
	out3 := &collectOutput{failAt: -1}
	for range 5 {
		if err := e3.Pump(out3); err != nil {
			t.Fatal(err)
		}
	}
	if got := records(t, out3.events); len(got) != 2 {
		t.Fatalf("new consumer must start from the beginning: %#v", got)
	}
}

// A new joiner picks its starting range and sees which earlier records are
// already unavailable because the source rotated while nobody read them.
func TestJoinPolicyAndUnavailableRanges(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	consumerPath := filepath.Join(dir, "consumers.json")
	if err := os.WriteFile(logPath, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cs := NewConsumerStore(consumerPath)
	veteran := newConsumerEngine(t, "veteran", "app", logPath, checkpointPath, cs)
	out := &collectOutput{failAt: -1}
	if err := veteran.Pump(out); err != nil {
		t.Fatal(err)
	}

	// Rotation while collection is paused: the confirmed file disappears.
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("fresh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := veteran.Pump(out); err != nil {
		t.Fatal(err)
	}

	late, err := cs.Join("late", "app", logPath, JoinNewest)
	if err != nil {
		t.Fatal(err)
	}
	if late.Rejoined {
		t.Fatal("first join must not be reported as a rejoin")
	}
	if late.Start.Offset != 6 {
		t.Fatalf("JoinNewest must start at the current end: %+v", late.Start)
	}
	if len(late.Unavailable) != 1 || late.Unavailable[0].Reason != "rotated_away" {
		t.Fatalf("rotated-away range not reported to joiner: %+v", late.Unavailable)
	}
	if late.Unavailable[0].From != 4 {
		t.Fatalf("gap should start at the unconfirmed offset: %+v", late.Unavailable[0])
	}

	early, err := cs.Join("early", "app", logPath, JoinOldest)
	if err != nil {
		t.Fatal(err)
	}
	if early.Start.Offset != 0 {
		t.Fatalf("JoinOldest must start at zero: %+v", early.Start)
	}
	if len(early.Unavailable) != 1 {
		t.Fatalf("unavailable ranges must be visible regardless of policy: %+v", early.Unavailable)
	}

	// The late joiner only sees new records; the early joiner reads the
	// current file from the start. Neither can get the rotated "old" back.
	lateEngine := newConsumerEngine(t, "late", "app", logPath, checkpointPath, cs)
	lateOut := &collectOutput{failAt: -1}
	for range 5 {
		if err := lateEngine.Pump(lateOut); err != nil {
			t.Fatal(err)
		}
	}
	if got := records(t, lateOut.events); len(got) != 0 {
		t.Fatalf("late joiner should skip existing content: %#v", got)
	}
	earlyEngine := newConsumerEngine(t, "early", "app", logPath, checkpointPath, cs)
	earlyOut := &collectOutput{failAt: -1}
	for range 5 {
		if err := earlyEngine.Pump(earlyOut); err != nil {
			t.Fatal(err)
		}
	}
	got := records(t, earlyOut.events)
	if len(got) != 1 || string(got[0].Raw) != "fresh" {
		t.Fatalf("early joiner should read the current file: %#v", got)
	}
}

// Abandoning a range relieves retention pressure, leaves the range and reason
// on record, and is never counted as successfully processed work.
func TestAbandonLeavesRangeAndReason(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	consumerPath := filepath.Join(dir, "consumers.json")
	if err := os.WriteFile(logPath, []byte("a\nb\nc\nd\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cs := NewConsumerStore(consumerPath)
	e := newSlowConsumerEngine(t, "busy", "app", logPath, checkpointPath, cs)
	out := &collectOutput{failAt: 1}
	var pumpErr error
	for range 8 {
		if pumpErr = e.Pump(out); pumpErr != nil {
			break
		}
	}
	if pumpErr == nil {
		t.Fatal("expected publish failure")
	}

	if err := e.Abandon(6, ""); err == nil {
		t.Fatal("abandon without a reason must fail")
	}
	if err := e.Abandon(6, "retention pressure from long pause"); err != nil {
		t.Fatal(err)
	}

	abandoned, err := cs.Abandonments("busy")
	if err != nil {
		t.Fatal(err)
	}
	if len(abandoned) != 1 || abandoned[0].From != 2 || abandoned[0].To != 6 ||
		abandoned[0].Reason != "retention pressure from long pause" {
		t.Fatalf("abandonment did not record range and reason: %+v", abandoned)
	}
	status := e.RecoveryStatus()
	if len(status.Abandoned) != 1 {
		t.Fatalf("abandoned range must be visible in recovery status: %+v", status)
	}
	if status.Confirmed.Offset != 6 {
		t.Fatalf("checkpoint should move past the abandoned range: %+v", status.Confirmed)
	}

	// Only records after the abandoned range are delivered; the skipped
	// records never appear as processed output.
	e2 := newConsumerEngine(t, "busy", "app", logPath, checkpointPath, cs)
	out2 := &collectOutput{failAt: -1}
	for range 5 {
		if err := e2.Pump(out2); err != nil {
			t.Fatal(err)
		}
	}
	got := records(t, out2.events)
	if len(got) != 1 || string(got[0].Raw) != "d" {
		t.Fatalf("abandoned records must not be delivered: %#v", got)
	}
	confirmed := records(t, out.events)
	if len(confirmed) != 1 || string(confirmed[0].Raw) != "a" {
		t.Fatalf("confirmed prefix wrong: %#v", confirmed)
	}
}

// An unreadable source fails only its own consumer engine; other sources keep
// flowing, and the gap left by the outage is shown on recovery.
func TestConsumerUnreadableSourceGapVisible(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "state.json")
	consumerPath := filepath.Join(dir, "consumers.json")
	blockedPath := filepath.Join(dir, "blocked.log")
	okPath := filepath.Join(dir, "ok.log")
	if err := os.WriteFile(blockedPath, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(okPath, []byte("ok-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cs := NewConsumerStore(consumerPath)
	blocked := newConsumerEngine(t, "team", "blocked", blockedPath, checkpointPath, cs)
	ok := newConsumerEngine(t, "team", "ok", okPath, checkpointPath, cs)
	blockedOut := &collectOutput{failAt: -1}
	okOut := &collectOutput{failAt: -1}
	if err := blocked.Pump(blockedOut); err != nil {
		t.Fatal(err)
	}
	if err := ok.Pump(okOut); err != nil {
		t.Fatal(err)
	}

	// The source becomes unreadable, then is rotated away during the outage.
	if err := os.Chmod(blockedPath, 0o000); err != nil {
		t.Fatal(err)
	}
	if err := blocked.Pump(blockedOut); err == nil {
		t.Fatal("expected error for unreadable source")
	}
	f, err := os.OpenFile(okPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("ok-2\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := ok.Pump(okOut); err != nil {
		t.Fatalf("healthy source blocked by unreadable one: %v", err)
	}
	if got := records(t, okOut.events); len(got) != 2 || string(got[1].Raw) != "ok-2" {
		t.Fatalf("healthy source did not continue: %#v", got)
	}

	if err := os.Rename(blockedPath, blockedPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blockedPath, []byte("fresh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := blocked.Pump(blockedOut); err != nil {
		t.Fatal(err)
	}

	status := blocked.RecoveryStatus()
	if len(status.Gaps) != 1 || status.Gaps[0].Reason != "rotated_away" {
		t.Fatalf("outage gap not shown: %+v", status.Gaps)
	}
	if status.Gaps[0].From != 6 {
		t.Fatalf("gap should start at the confirmed offset: %+v", status.Gaps[0])
	}
	if status.Gaps[0].Consumer != "team" || status.Gaps[0].Stream != "blocked" {
		t.Fatalf("gap not attributed to the affected consumer: %+v", status.Gaps[0])
	}
	got := records(t, blockedOut.events)
	if len(got) != 2 || string(got[1].Raw) != "fresh" {
		t.Fatalf("recovered source resumed wrong: %#v", got)
	}
	if strings.Contains(got[1].ID, got[0].ID) {
		t.Fatal("record identity must change across rotation")
	}
}
