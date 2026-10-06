package logengine

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The same byte sequence must produce the same records (identity and order)
// no matter how the reads are chunked, and a confirmed checkpoint must only
// ever cover records that were actually delivered or recorded as bad.
func TestRecordsIndependentOfReadSize(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	content := "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	var want []Record
	for _, readSize := range []int{4096, 1, 2, 3, 5, 7} {
		checkpointPath := filepath.Join(dir, "state.json")
		os.Remove(checkpointPath)
		e, err := NewEngine("app", logPath, checkpointPath, NewJSONParser(), WithReadSize(readSize))
		if err != nil {
			t.Fatal(err)
		}
		out := &collectOutput{failAt: -1}
		for i := 0; i < 20; i++ {
			if err := e.Pump(out); err != nil {
				t.Fatalf("read size %d: %v", readSize, err)
			}
		}
		got := records(t, out.events)
		if len(got) != 3 {
			t.Fatalf("read size %d: got %d records, want 3", readSize, len(got))
		}
		if want == nil {
			want = got
		}
		for i := range got {
			if got[i].ID != want[i].ID || string(got[i].Raw) != string(want[i].Raw) {
				t.Fatalf("read size %d: record %d differs: %+v vs %+v", readSize, i, got[i], want[i])
			}
		}
		status := e.RecoveryStatus()
		if status.Confirmed.Offset != int64(len(content)) {
			t.Fatalf("read size %d: confirmed offset %d, want %d", readSize, status.Confirmed.Offset, len(content))
		}
		e.Close()
	}
}

// Two independent consumers reading the same source in small chunks: a fast
// consumer that keeps up and a slow one that pauses across a rotation and a
// truncation. One consumer's confirmations must never advance the other, and
// reopened consumers must see identical record identities.
func TestFastSlowConsumersSmallChunksLifecycle(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	consumersPath := filepath.Join(dir, "consumers.json")
	store := NewConsumerStore(consumersPath)
	parser := func() StreamingParser {
		return NewMultilineParser(RegexpStart(regexp.MustCompile("^{")), 1<<20, nil)
	}

	write := func(data string) {
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(data); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	write("{\"n\":1}\ncontinued\n{\"n\":2}\n")

	newEngine := func(consumer string) *Engine {
		e, err := NewEngine("app", logPath, filepath.Join(dir, "state.json"), parser(),
			WithReadSize(4), WithMaxLineBytes(1<<20), WithConsumer(consumer, store))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	pumpAll := func(e *Engine, out *collectOutput) {
		t.Helper()
		for i := 0; i < 50; i++ {
			if err := e.Pump(out); err != nil {
				t.Fatal(err)
			}
		}
	}

	fast, fastOut := newEngine("fast"), &collectOutput{failAt: -1}
	slow, slowOut := newEngine("slow"), &collectOutput{failAt: -1}
	pumpAll(fast, fastOut)
	pumpAll(slow, slowOut)

	fastRecs := records(t, fastOut.events)
	slowRecs := records(t, slowOut.events)
	if len(fastRecs) != 2 || len(slowRecs) != 2 {
		t.Fatalf("fast=%d slow=%d records, want 2 each", len(fastRecs), len(slowRecs))
	}
	for i := range fastRecs {
		if fastRecs[i].ID != slowRecs[i].ID {
			t.Fatalf("record %d identity differs between consumers", i)
		}
	}

	// Slow consumer pauses; fast consumer keeps going through more data.
	slow.Close()
	slowCpBefore, err := store.Checkpoints("slow")
	if err != nil {
		t.Fatal(err)
	}
	write("{\"n\":3}\n")
	pumpAll(fast, fastOut)

	// Rotation while slow is away.
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	os.Remove(logPath)
	write("{\"n\":4}\n")
	pumpAll(fast, fastOut)

	// The fast consumer's confirmations must not have moved the slow
	// consumer's persisted checkpoint while it was away.
	slowCpAfter, err := store.Checkpoints("slow")
	if err != nil {
		t.Fatal(err)
	}
	if !checkpointsEqual(slowCpAfter["app"], slowCpBefore["app"]) {
		t.Fatalf("fast consumer confirmations advanced slow consumer: %+v -> %+v",
			slowCpBefore["app"], slowCpAfter["app"])
	}

	// Slow reopens: it resumes from its own checkpoint, sees the rotated-away
	// range as a gap, and receives only the records it had not confirmed.
	slow = newEngine("slow")
	pumpAll(slow, slowOut)
	slowStatus := slow.RecoveryStatus()
	if len(slowStatus.Gaps) == 0 {
		t.Fatal("slow consumer should see the rotated-away range as a gap")
	}
	slowRecs = records(t, slowOut.events)
	if len(slowRecs) != 3 {
		t.Fatalf("slow consumer got %d records, want 3: %+v", len(slowRecs), slowRecs)
	}

	// Truncation is reported as a bad record with position and reason, and
	// reading restarts at zero for both consumers independently.
	if err := os.Truncate(logPath, 2); err != nil {
		t.Fatal(err)
	}
	pumpAll(fast, fastOut)
	pumpAll(slow, slowOut)
	if got := bad(fastOut.events); len(got) == 0 || got[len(got)-1].Reason != "file_truncated" {
		t.Fatalf("fast consumer missing truncation bad record: %+v", bad(fastOut.events))
	}
	if got := bad(slowOut.events); len(got) == 0 || got[len(got)-1].Reason != "file_truncated" {
		t.Fatalf("slow consumer missing truncation bad record: %+v", bad(slowOut.events))
	}

	// Abandoning a range stays separate from confirmed progress and is
	// visible only to the consumer that gave it up.
	if err := slow.Abandon(1, "retention"); err != nil {
		t.Fatal(err)
	}
	slowAb, err := store.Abandonments("slow")
	if err != nil {
		t.Fatal(err)
	}
	if len(slowAb) != 1 || slowAb[0].Reason != "retention" || slowAb[0].To != 1 {
		t.Fatalf("abandonment not recorded with range and reason: %+v", slowAb)
	}
	fastAb, err := store.Abandonments("fast")
	if err != nil {
		t.Fatal(err)
	}
	if len(fastAb) != 0 {
		t.Fatalf("slow consumer's abandonment leaked to fast consumer: %+v", fastAb)
	}
	slow.Close()
	fast.Close()
}

func checkpointsEqual(a, b Checkpoint) bool {
	return a.FileID == b.FileID && a.Offset == b.Offset && a.NextLine == b.NextLine &&
		string(a.ParserState) == string(b.ParserState)
}
