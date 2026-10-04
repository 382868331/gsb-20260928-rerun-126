package logengine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func consumerEngine(t *testing.T, consumer, stream, logPath, checkpointPath string, opts ...Option) *Engine {
	t.Helper()
	opts = append([]Option{WithConsumer(consumer)}, opts...)
	e, err := NewEngine(stream, logPath, checkpointPath, NewMultilineParser(AlwaysStart, 100, nil), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func pumpAll(t *testing.T, e *Engine, out *countingOutput) {
	t.Helper()
	for range 20 {
		if err := e.Pump(out); err != nil {
			t.Fatal(err)
		}
	}
}

func raws(events []Event) []string {
	var out []string
	for _, event := range events {
		if record, ok := event.(Record); ok {
			out = append(out, string(record.Raw))
		}
	}
	return out
}

func join(values []string) string { return strings.Join(values, ",") }

// A fast consumer confirming batches must not move the slow consumer's
// progress; both see every record, each at its own pace.
func TestIndependentConsumersAckSeparately(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("r1\nr2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	fast := consumerEngine(t, "fast", "app", logPath, checkpointPath)
	slow := consumerEngine(t, "slow", "app", logPath, checkpointPath)
	fastOut, slowOut := newCountingOutput(), newCountingOutput()

	pumpAll(t, fast, fastOut)
	if got := join(raws(fastOut.order)); got != "r1,r2" {
		t.Fatalf("fast consumer records: %s", got)
	}

	// The fast consumer's confirmations are stored under its own identity.
	states, err := NewStateStore(checkpointPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if states["fast/app"].Offset != 6 {
		t.Fatalf("fast checkpoint wrong: %+v", states)
	}
	if _, ok := states["slow/app"]; ok {
		t.Fatalf("fast consumer advanced the slow consumer: %+v", states)
	}
	if status := slow.RecoveryStatus(); status.Confirmed.Offset != 0 {
		t.Fatalf("slow progress moved by fast consumer: %+v", status)
	}

	// The slow consumer still receives the whole history, with the same
	// stable record identities the fast consumer saw.
	pumpAll(t, slow, slowOut)
	if got := join(raws(slowOut.order)); got != "r1,r2" {
		t.Fatalf("slow consumer records: %s", got)
	}
	for i, event := range slowOut.order {
		if event.(Record).ID != fastOut.order[i].(Record).ID {
			t.Fatalf("record identity differs between consumers: %q vs %q",
				event.(Record).ID, fastOut.order[i].(Record).ID)
		}
	}

	// New content is delivered to each consumer exactly from its own offset.
	f, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	f.WriteString("r3\n")
	f.Close()
	pumpAll(t, fast, fastOut)
	pumpAll(t, slow, slowOut)
	if got := join(raws(fastOut.order)); got != "r1,r2,r3" {
		t.Fatalf("fast consumer after append: %s", got)
	}
	if got := join(raws(slowOut.order)); got != "r1,r2,r3" {
		t.Fatalf("slow consumer after append: %s", got)
	}
}

// A paused consumer resumes where it left off, a rejoining consumer restores
// its persisted progress, and a brand-new consumer may choose its start
// range with the skipped prefix reported as unavailable.
func TestConsumerPauseResumeAndRejoinChoices(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("r1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	slow := consumerEngine(t, "slow", "app", logPath, checkpointPath)
	slowOut := newCountingOutput()
	pumpAll(t, slow, slowOut)
	if got := join(raws(slowOut.order)); got != "r1" {
		t.Fatalf("initial records: %s", got)
	}
	if err := slow.Close(); err != nil {
		t.Fatal(err)
	}

	// Pause window: more data arrives while the consumer is gone.
	f, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	f.WriteString("r2\n")
	f.Close()

	// Rejoining (new process, same identity) resumes at the persisted
	// checkpoint: r1 is not redelivered, r2 is.
	rejoined := consumerEngine(t, "slow", "app", logPath, checkpointPath)
	rejoinedOut := newCountingOutput()
	pumpAll(t, rejoined, rejoinedOut)
	if got := join(raws(rejoinedOut.order)); got != "r2" {
		t.Fatalf("rejoined consumer resumed wrong: %s", got)
	}

	// A new consumer joining at the latest offset skips the existing content
	// and sees exactly which range is no longer delivered to it.
	late := consumerEngine(t, "late", "app", logPath, checkpointPath, WithStartFromLatest())
	status := late.RecoveryStatus()
	if len(status.Gaps) != 1 || status.Gaps[0].Reason != "skipped_on_join" ||
		status.Gaps[0].From != 0 || status.Gaps[0].To != 6 {
		t.Fatalf("join skip not reported as gap: %+v", status.Gaps)
	}
	lateOut := newCountingOutput()
	pumpAll(t, late, lateOut)
	if len(lateOut.order) != 0 {
		t.Fatalf("late joiner received skipped history: %#v", lateOut.order)
	}

	// A new consumer joining at the oldest available offset receives all of
	// the history the other consumers already confirmed.
	full := consumerEngine(t, "full", "app", logPath, checkpointPath)
	fullOut := newCountingOutput()
	pumpAll(t, full, fullOut)
	if got := join(raws(fullOut.order)); got != "r1,r2" {
		t.Fatalf("oldest-join consumer records: %s", got)
	}

	// After the join choices, new data flows to everyone from their own
	// position, and a restart restores each consumer independently.
	f, _ = os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	f.WriteString("r3\n")
	f.Close()
	for _, e := range []*Engine{rejoined, late, full} {
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	restarted := map[string]*countingOutput{}
	for _, name := range []string{"slow", "late", "full"} {
		e := consumerEngine(t, name, "app", logPath, checkpointPath)
		out := newCountingOutput()
		pumpAll(t, e, out)
		restarted[name] = out
	}
	for name, out := range restarted {
		if got := join(raws(out.order)); got != "r3" {
			t.Fatalf("consumer %s after restart: %s", name, got)
		}
	}
}

// Abandoning a historical range releases retention without faking success:
// nothing is published, the checkpoint moves past the range, and a durable
// audit entry keeps the range and reason visible after restart.
func TestAbandonReleasesRangeWithReason(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	if err := os.WriteFile(logPath, []byte("h1\nh2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	busy := consumerEngine(t, "busy", "app", logPath, checkpointPath)
	if _, err := busy.Abandon(""); err == nil {
		t.Fatal("abandon without a reason must fail")
	}
	entry, err := busy.Abandon("paused 30d, releasing retention")
	if err != nil {
		t.Fatal(err)
	}
	if entry.From != 0 || entry.To != 6 || entry.Reason != "paused 30d, releasing retention" {
		t.Fatalf("abandonment range or reason wrong: %+v", entry)
	}

	// The abandoned range is never published as if it had been processed.
	out := newCountingOutput()
	pumpAll(t, busy, out)
	if len(out.order) != 0 {
		t.Fatalf("abandoned records published as success: %#v", out.order)
	}

	// The gap stays visible to the consumer, across a restart, with its
	// reason attached.
	status := busy.RecoveryStatus()
	if len(status.Gaps) != 1 || !strings.HasPrefix(status.Gaps[0].Reason, "abandoned: ") {
		t.Fatalf("abandoned range not reported as gap: %+v", status.Gaps)
	}
	if err := busy.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := consumerEngine(t, "busy", "app", logPath, checkpointPath)
	abs := restarted.Abandonments()
	if len(abs) != 1 || abs[0].From != 0 || abs[0].To != 6 || abs[0].Consumer != "busy" {
		t.Fatalf("abandonment not persisted: %+v", abs)
	}

	// New content after the abandonment flows normally from the new offset.
	f, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	f.WriteString("h3\n")
	f.Close()
	restartedOut := newCountingOutput()
	pumpAll(t, restarted, restartedOut)
	if got := join(raws(restartedOut.order)); got != "h3" {
		t.Fatalf("post-abandon records: %s", got)
	}

	// Other consumers are untouched by the abandonment.
	other := consumerEngine(t, "other", "app", logPath, checkpointPath)
	otherOut := newCountingOutput()
	pumpAll(t, other, otherOut)
	if got := join(raws(otherOut.order)); got != "h1,h2,h3" {
		t.Fatalf("abandon leaked into another consumer: %s", got)
	}
}

// One unreadable source fails on its own status only: the group keeps
// pumping the healthy sources and reports the affected gap for the broken
// one.
func TestGroupContinuesWhenSourceUnreadable(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "state.json")
	blockedPath := filepath.Join(dir, "blocked.log")
	okPath := filepath.Join(dir, "ok.log")
	if err := os.WriteFile(blockedPath, []byte("b1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(okPath, []byte("o1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	blocked := consumerEngine(t, "worker", "blocked", blockedPath, checkpointPath)
	ok := consumerEngine(t, "worker", "ok", okPath, checkpointPath)
	blockedOut, okOut := newCountingOutput(), newCountingOutput()
	group := NewGroup()
	group.Add(blocked, blockedOut)
	group.Add(ok, okOut)

	statuses := group.PumpAll()
	for _, status := range statuses {
		if status.Err != nil {
			t.Fatalf("initial pump failed for %s: %v", status.Stream, status.Err)
		}
	}

	// More content arrives, then one source becomes unreadable.
	for _, path := range []string{blockedPath, okPath} {
		f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		f.WriteString("next\n")
		f.Close()
	}
	if err := os.Chmod(blockedPath, 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(blockedPath, 0o600)

	statuses = group.PumpAll()
	var blockedStatus, okStatus *SourceStatus
	for i := range statuses {
		switch statuses[i].Stream {
		case "blocked":
			blockedStatus = &statuses[i]
		case "ok":
			okStatus = &statuses[i]
		}
	}
	if blockedStatus.Err == nil {
		t.Fatal("unreadable source did not fail")
	}
	if len(blockedStatus.Gaps) == 0 || blockedStatus.Gaps[0].Reason != "source_unreadable" {
		t.Fatalf("unreadable source gap not reported: %+v", blockedStatus.Gaps)
	}
	if okStatus.Err != nil {
		t.Fatalf("healthy source blocked by unreadable one: %v", okStatus.Err)
	}
	if got := join(raws(okOut.order)); got != "o1,next" {
		t.Fatalf("healthy source records: %s", got)
	}

	// Once readable again the source resumes without skipping its backlog.
	if err := os.Chmod(blockedPath, 0o600); err != nil {
		t.Fatal(err)
	}
	statuses = group.PumpAll()
	if statuses[0].Err != nil {
		t.Fatalf("recovered source still failing: %v", statuses[0].Err)
	}
	if got := join(raws(blockedOut.order)); got != "b1,next" {
		t.Fatalf("recovered source skipped backlog: %s", got)
	}
}
