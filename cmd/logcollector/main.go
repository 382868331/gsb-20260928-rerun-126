// Command logcollector tails local multiline log files, parses them through a
// pluggable chain and republishes records, persisting positions only after the
// output acknowledges each record.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"logengine/pkg/engine"
	"logengine/pkg/parser"
	"logengine/pkg/position"
)

// stdoutOutput is an example output: every published record is acknowledged
// once written and synced to stdout.
type stdoutOutput struct{ l *log.Logger }

func (o stdoutOutput) Publish(rec parser.Record) error {
	switch rec.Kind {
	case parser.KindGood:
		o.l.Printf("ACK good  %s gen=%d off=%d-%d fields=%v", rec.Source, rec.Gen, rec.Start, rec.End, rec.Fields)
	default:
		o.l.Printf("ACK bad   %s gen=%d off=%d-%d reason=%q sample=%q", rec.Source, rec.Gen, rec.Start, rec.End, rec.Reason, rec.RedactedSample)
	}
	return nil
}

func main() {
	var (
		positionFile = flag.String("positions", "positions.json", "durable position store")
		pollInterval = flag.Duration("poll", 200*time.Millisecond, "poll interval")
		flushAfter   = flag.Duration("flush", 5*time.Second, "half-record flush timeout")
		maxRecord    = flag.Int("max-record-bytes", 1<<20, "max assembled multiline record")
		startRe      = flag.String("multiline-start", `^\d{4}-\d{2}-\d{2}`, "regex marking the first line of a record")
	)
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: logcollector [flags] <logfile> [<logfile>...]")
		os.Exit(2)
	}
	start, err := regexp.Compile(*startRe)
	if err != nil {
		log.Fatalf("invalid multiline-start regex: %v", err)
	}
	store, err := position.Open(*positionFile)
	if err != nil {
		log.Fatal(err)
	}
	var sources []engine.SourceConfig
	for _, p := range flag.Args() {
		sources = append(sources, engine.SourceConfig{
			Path:      p,
			Multiline: parser.MultilineConfig{StartReg: start, MaxBytes: *maxRecord},
		})
	}
	e := engine.New(engine.Config{
		Sources: sources,
		Store:   store,
		Out:     stdoutOutput{l: log.New(os.Stdout, "", log.LstdFlags)},
		// Parsers are an ordered chain; append new parsing methods here.
		Chain:      parser.NewChain(parser.JSONParser{}),
		PollEvery:  *pollInterval,
		FlushAfter: *flushAfter,
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := e.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
