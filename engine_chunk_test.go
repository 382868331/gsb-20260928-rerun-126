package logengine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Two legal JSON records MUST be delivered identically no matter how the
// read buffer splits their bytes. Earlier the engine reopened the source at
// the confirmed offset while a partial next line was already buffered, so the
// same bytes were spliced twice and the second record arrived as an
// invalid_json bad record whose acknowledgement then skipped past it.
func TestJSONRecordsIdenticalAcrossReadSizes(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	content := "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	var baseline []string
	for readSize := 1; readSize <= len(content)+1; readSize++ {
		checkpointPath := filepath.Join(dir, fmt.Sprintf("state-%d.json", readSize))
		e, err := NewEngine("app", logPath, checkpointPath, NewJSONParser(), WithReadSize(readSize))
		if err != nil {
			t.Fatal(err)
		}
		out := &collectOutput{failAt: -1}
		for range 200 {
			if err := e.Pump(out); err != nil {
				t.Fatalf("readSize %d: %v", readSize, err)
			}
			if len(out.events) == 3 {
				break
			}
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		got := records(t, out.events)
		if len(got) != 3 {
			t.Fatalf("readSize %d delivered %d records: %#v", readSize, len(got), out.events)
		}
		sig := make([]string, len(got))
		for i, record := range got {
			sig[i] = record.ID + ":" + string(record.Raw)
		}
		if readSize == 1 {
			baseline = sig
		} else {
			for i := range sig {
				if sig[i] != baseline[i] {
					t.Fatalf("readSize %d changed record %d: %q != %q", readSize, i, sig[i], baseline[i])
				}
			}
		}
	}
}

// After reopening mid-file the confirmed offset must still point at real
// deliveries, and the unread record must be recovered with the same identity
// it would have had on a single uninterrupted pass.
func TestSmallReadReopenDoesNotSkipRecord(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	checkpointPath := filepath.Join(dir, "state.json")
	content := "{\"a\":1}\n{\"b\":2}\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	e, err := NewEngine("app", logPath, checkpointPath, NewJSONParser(), WithReadSize(3))
	if err != nil {
		t.Fatal(err)
	}
	first := &collectOutput{failAt: -1}
	for {
		if err := e.Pump(first); err != nil {
			t.Fatal(err)
		}
		if len(first.events) > 0 {
			break
		}
	}
	if got := records(t, first.events); len(got) != 1 || string(got[0].Raw) != `{"a":1}` {
		t.Fatalf("first batch wrong: %#v", first.events)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine("app", logPath, checkpointPath, NewJSONParser(), WithReadSize(3))
	if err != nil {
		t.Fatal(err)
	}
	second := &collectOutput{failAt: -1}
	for range 100 {
		if err := e2.Pump(second); err != nil {
			t.Fatal(err)
		}
		if len(second.events) == 1 {
			break
		}
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	got := records(t, second.events)
	if len(got) != 1 || string(got[0].Raw) != `{"b":2}` {
		t.Fatalf("second record lost or corrupted after reopen: %#v", second.events)
	}
	want := stableRecordID("app", got[0].Position.FileID, 8, 16)
	if got[0].ID != want {
		t.Fatalf("record identity changed across reopen: %q != %q", got[0].ID, want)
	}
}
