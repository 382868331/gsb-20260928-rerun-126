package parser

import (
	"regexp"
	"strings"
	"testing"
)

func TestRedactSecrets(t *testing.T) {
	cases := []struct{ in, leak string }{
		{`user=a password=hunter2 ok`, "hunter2"},
		{`token: abcdef123456`, "abcdef123456"},
		{`{"api_key":"supersecret"}`, "supersecret"},
		{`Authorization: Bearer eyJ.AAA.BBB`, "eyJ.AAA.BBB"},
		{`Authorization=Basic dXNlcjpwYXNz`, "dXNlcjpwYXNz"},
	}
	for _, c := range cases {
		got := Redact(c.in)
		if strings.Contains(got, c.leak) {
			t.Errorf("Redact(%q) leaked %q -> %q", c.in, c.leak, got)
		}
		if !strings.Contains(got, "***") {
			t.Errorf("Redact(%q) did not mask: %q", c.in, got)
		}
	}
}

func TestRedactPreservesStructure(t *testing.T) {
	got := Redact(`{"user":"alice","password":"hunter2","n":1}`)
	if !strings.Contains(got, `"user":"alice"`) || !strings.Contains(got, `"password":"***"`) {
		t.Fatalf("json structure damaged: %q", got)
	}
}

func TestSplitterMultilineAndOverflow(t *testing.T) {
	s := NewSplitter(MultilineConfig{StartReg: regexp.MustCompile(`^T`), MaxBytes: 32})

	items := s.Feed([]byte("T one\n cont1\nT two\n"), 0)
	if len(items) != 1 {
		t.Fatalf("want 1 closed item, got %d", len(items))
	}
	if items[0].Text != "T one\n cont1" {
		t.Fatalf("multiline assembly wrong: %q", items[0].Text)
	}
	last := s.Flush(int64(len("T one\n cont1\nT two\n")))
	if last == nil || last.Text != "T two" {
		t.Fatalf("final record via flush, got %+v", last)
	}
	if items[0].End != last.Start {
		t.Fatalf("offsets must be contiguous: %d -> %d", items[0].End, last.Start)
	}

	big := []byte("T " + strings.Repeat("y", 100) + "\nT small\n")

	items = s.Feed(big, 0)
	if len(items) != 1 {
		t.Fatalf("overflow item closes when next record starts, got %d", len(items))
	}
	if !items[0].Overflow {
		t.Fatal("first item must be flagged overflow")
	}
	if items[0].End != int64(len(big)-len("T small\n")) {
		t.Fatalf("overflow item must cover dropped bytes: end=%d", items[0].End)
	}
	if len(items[0].Text) > 32 {
		t.Fatalf("buffered text must be bounded, got %d", len(items[0].Text))
	}
	tail := s.Flush(int64(len(big)))
	if tail == nil || tail.Text != "T small" {
		t.Fatalf("second record via flush, got %+v", tail)
	}
	if items[0].End != tail.Start {
		t.Fatal("positions must remain exact across overflow")
	}
}

func TestSplitterFlushAtBoundary(t *testing.T) {
	s := NewSplitter(MultilineConfig{StartReg: regexp.MustCompile(`^T`), MaxBytes: 1024})
	s.Feed([]byte("T half\n cont"), 0)
	item := s.Flush(int64(len("T half\n cont")))
	if item == nil || !item.Incomplete || item.Text != "T half\n cont" {
		t.Fatalf("flush must publish partial record, got %+v", item)
	}
	if s.HasPending() {
		t.Fatal("state must be clear after flush")
	}
	s.Reset()
}
