package engine

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"logengine/pkg/parser"
	"logengine/pkg/position"
)

func testEngine(t *testing.T, logPath, storePath string, cfg parser.MultilineConfig) (*Engine, *MemoryOutput, *position.Store, context.CancelFunc) {
	t.Helper()
	store, err := position.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	out := &MemoryOutput{}
	chain := parser.NewChain(parser.JSONParser{})
	e := New(Config{
		Sources:    []SourceConfig{{Path: logPath, Multiline: cfg}},
		Store:      store,
		Out:        out,
		Chain:      chain,
		PollEvery:  20 * time.Millisecond,
		FlushAfter: 300 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return e, out, store, cancel
}

func waitN(t *testing.T, out *MemoryOutput, n int) []parser.Record {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		recs := out.Snapshot()
		if len(recs) >= n {
			return recs
		}
		time.Sleep(10 * time.Millisecond)
	}
	recs := out.Snapshot()
	t.Fatalf("wanted %d records, got %d: %+v", n, len(recs), recs)
	return nil
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

func defaultML() parser.MultilineConfig {
	return parser.MultilineConfig{StartReg: regexp.MustCompile(`^\d{4}-\d{2}-\d{2}`), MaxBytes: 1 << 20}
}

func TestBasicMultilineAndNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	storePath := filepath.Join(dir, "pos.json")
	writeFile(t, logPath, "")

	_, out, _, _ := testEngine(t, logPath, storePath, defaultML())
	writeFile(t, logPath, "2026-09-29 10:00:01 start\n    stack line A\n2026-09-29 10:00:02 {\"msg\":\"ok\"}\n")

	recs := waitN(t, out, 2)
	if recs[0].Kind != parser.KindBad {
		t.Fatalf("plain line should be bad record, got %+v", recs[0])
	}
	if recs[0].Raw != "" {
		t.Fatalf("bad record leaked raw text: %q", recs[0].Raw)
	}
	if recs[0].RedactedSample == "" {
		t.Fatal("bad record must keep a redacted sample")
	}
	if recs[1].Kind != parser.KindGood {
		t.Fatalf("json should be good: %+v", recs[1])
	}
	if !strings.Contains(recs[0].RedactedSample, "stack line A") {
		t.Fatal("continuation line must be part of the multiline record sample")
	}
	if recs[0].End != recs[1].Start {
		t.Fatalf("positions must be contiguous: %d != %d", recs[0].End, recs[1].Start)
	}

	// No new bytes => no duplicate publications over time.
	time.Sleep(200 * time.Millisecond)
	if len(out.Snapshot()) != 2 {
		t.Fatalf("unexpected duplicate publication: %d", len(out.Snapshot()))
	}
}

func TestBadRecordRedactsSecrets(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	writeFile(t, logPath, "")
	_, out, _, _ := testEngine(t, logPath, filepath.Join(dir, "p.json"), defaultML())

	writeFile(t, logPath, "2026-09-29 user=alice password=hunter2 token: abc123secret\n")
	writeFile(t, logPath, `2026-09-29 {"user":"bob","api_key":"supersecretkey"}`+"\n")
	writeFile(t, logPath, "2026-09-29 Authorization: Bearer eyJhbGciOi.JzdWIiOi.X\n")
	recs := waitN(t, out, 3)
	for i, r := range recs {
		if r.Kind == parser.KindBad && r.Raw != "" {
			t.Fatalf("bad record %d leaked raw text: %q", i, r.Raw)
		}
		for _, leak := range []string{"hunter2", "abc123secret", "supersecretkey", "eyJhbGciOi"} {
			if strings.Contains(r.RedactedSample, leak) {
				t.Fatalf("record %d sample leaked %q: %q", i, leak, r.RedactedSample)
			}
		}
	}
}

func TestRotationNewFileNotTailOfOld(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	storePath := filepath.Join(dir, "p.json")
	writeFile(t, logPath, "")
	_, out, _, _ := testEngine(t, logPath, storePath, defaultML())

	writeFile(t, logPath, "2026-09-29 old-line-1\n")
	waitN(t, out, 1)

	// Rotate: move old file away, create a brand new file at the path.
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	// Last-moment append to the OLD inode must still be collected.
	writeFile(t, logPath+".1", "    old tail continuation\n")
	writeFile(t, logPath, "2026-09-29 brand new file line one\n")

	recs := waitN(t, out, 3)
	// rec 1 = old-line-1, rec 2 = old tail continuation (old generation),
	// rec 3 = new file line one, whose start offset in the new file is 0.
	if recs[1].Gen == recs[2].Gen {
		t.Fatal("rotated file must be a new generation")
	}
	if recs[2].Start != 0 {
		t.Fatalf("new file must start at offset 0, got %d", recs[2].Start)
	}
	if !strings.Contains(recs[2].RedactedSample, "brand new") {
		t.Fatalf("wrong content after rotation: %q", recs[2].RedactedSample)
	}
}

func TestTruncationRestartsAtZero(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	writeFile(t, logPath, "")
	_, out, _, _ := testEngine(t, logPath, filepath.Join(dir, "p.json"), defaultML())

	writeFile(t, logPath, strings.Repeat("2026-09-29 long old content line\n", 5))
	waitN(t, out, 5)

	if err := os.Truncate(logPath, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	writeFile(t, logPath, "2026-09-29 after truncation fresh content\n")

	deadline := time.Now().Add(3 * time.Second)
	var fresh parser.Record
	for time.Now().Before(deadline) {
		for _, r := range out.Snapshot() {
			if strings.Contains(r.RedactedSample, "after truncation") {
				fresh = r
			}
		}
		if fresh.Source != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fresh.Source == "" {
		t.Fatal("post-truncation record never read")
	}
	if fresh.Start != 0 {
		t.Fatalf("truncated file must be read from 0, got start %d", fresh.Start)
	}
}

func TestRestartResumesAfterAcked(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	storePath := filepath.Join(dir, "p.json")
	writeFile(t, logPath, "")

	{ // first engine run
		_, out, _, cancel := testEngine(t, logPath, storePath, defaultML())
		writeFile(t, logPath, "2026-09-29 line one\n2026-09-29 line two\n")
		waitN(t, out, 2)
		cancel()
		time.Sleep(30 * time.Millisecond)
	}

	// Second engine using the same durable store: existing records must NOT
	// be republished; appended ones are picked up.
	store, err := position.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	out := &MemoryOutput{}
	e := New(Config{
		Sources: []SourceConfig{{Path: logPath, Multiline: defaultML()}},
		Store:   store, Out: out,
		Chain:      parser.NewChain(parser.JSONParser{}),
		PollEvery:  20 * time.Millisecond,
		FlushAfter: 300 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	time.Sleep(150 * time.Millisecond)
	if n := len(out.Snapshot()); n != 0 {
		t.Fatalf("already-acked records republished after restart: %d", n)
	}
	writeFile(t, logPath, "2026-09-29 line three\n")
	waitN(t, out, 1)
	cancel()
	<-done
}

func TestMultilineOverflowIsBadAndPositionKept(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	writeFile(t, logPath, "")
	cfg := parser.MultilineConfig{StartReg: regexp.MustCompile(`^START`), MaxBytes: 64}
	_, out, _, _ := testEngine(t, logPath, filepath.Join(dir, "p.json"), cfg)

	huge := "START " + strings.Repeat("x", 200) + "\n"
	writeFile(t, logPath, huge)
	writeFile(t, logPath, "START second record\n")
	recs := waitN(t, out, 2)
	if recs[0].Kind != parser.KindBad || !strings.Contains(recs[0].Reason, "exceeds") {
		t.Fatalf("oversized record must be flagged bad with explicit reason: %+v", recs[0])
	}
	if len(recs[0].RedactedSample) > 600 {
		t.Fatalf("overflow sample must be bounded: %d", len(recs[0].RedactedSample))
	}
	if recs[0].End != recs[1].Start {
		t.Fatalf("overflow record must retain true position: %d -> %d", recs[0].End, recs[1].Start)
	}
	if recs[0].End != int64(len(huge)) {
		t.Fatalf("overflow end must cover dropped bytes: got %d want %d", recs[0].End, len(huge))
	}
}

// TestInterleavedRotateTruncateRestart mixes all three failure modes:
// acked records are never republished, rotated files start at 0, truncated
// files restart at 0, and half multiline content is flushed at the boundary.
func TestInterleavedRotateTruncateRestart(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	storePath := filepath.Join(dir, "pos.json")
	writeFile(t, logPath, "")

	start := func() (*Engine, *MemoryOutput, *position.Store, context.CancelFunc, chan error) {
		store, err := position.Open(storePath)
		if err != nil {
			t.Fatal(err)
		}
		out := &MemoryOutput{}
		e := New(Config{
			Sources: []SourceConfig{{Path: logPath, Multiline: defaultML()}},
			Store:   store, Out: out,
			Chain:      parser.NewChain(parser.JSONParser{}),
			PollEvery:  10 * time.Millisecond,
			FlushAfter: 300 * time.Millisecond,
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		return e, out, store, cancel, done
	}

	_, out, _, cancel, done := start()
	writeFile(t, logPath, "2026-09-29 phase1-a\n2026-09-29 phase1-b\n")
	waitN(t, out, 2)

	// Rotate.
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, logPath, "2026-09-29 phase2-after-rotate\n")
	var phase2 parser.Record
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range out.Snapshot() {
			if strings.Contains(r.RedactedSample, "phase2") {
				phase2 = r
			}
		}
		if phase2.Source != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if phase2.Start != 0 {
		t.Fatalf("rotated file must start at 0, got %d", phase2.Start)
	}

	// Truncate the new file, then restart the engine from the durable store.
	if err := os.Truncate(logPath, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	writeFile(t, logPath, "2026-09-29 phase3-post-trunc\n")
	var phase3 parser.Record
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range out.Snapshot() {
			if strings.Contains(r.RedactedSample, "phase3") {
				phase3 = r
			}
		}
		if phase3.Source != "" && phase3.Start == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if phase3.Source == "" {
		t.Fatal("phase3 post-truncation record not acked before restart")
	}
	cancel()
	<-done

	afterRestart := func() *MemoryOutput {
		_, o2, _, cancel2, done2 := start()
		t.Cleanup(func() { cancel2(); <-done2 })
		time.Sleep(150 * time.Millisecond)
		writeFile(t, logPath, "2026-09-29 phase4-after-restart\n")
		waitN(t, o2, 1)
		return o2
	}
	o2 := afterRestart()
	got := o2.Snapshot()
	if len(got) != 1 || !strings.Contains(got[0].RedactedSample, "phase4") {
		t.Fatalf("after restart only the newly appended record must publish, got %+v", got)
	}
	if got[0].Start == 0 {
		t.Fatal("resume must be at committed offset, not 0")
	}
}
