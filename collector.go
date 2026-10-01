package logengine

import (
	"errors"
	"fmt"
)

type Source struct {
	Stream string
	Path   string
	Parser StreamingParser
}

type SourceError struct {
	Source Source
	Err    error
}

func (e SourceError) Error() string {
	return fmt.Sprintf("source %q (%s): %v", e.Source.Stream, e.Source.Path, e.Err)
}

func (e SourceError) Unwrap() error { return e.Err }

type Collector struct {
	sources []Source
	engines map[string]*Engine
}

func NewCollector(checkpointPath string, sources []Source, opts ...Option) (*Collector, error) {
	if len(sources) == 0 {
		return nil, errors.New("collector requires at least one source")
	}
	store := NewStateStore(checkpointPath)
	states, err := store.Load()
	if err != nil {
		return nil, err
	}
	c := &Collector{sources: append([]Source(nil), sources...), engines: map[string]*Engine{}}
	for _, source := range sources {
		if source.Stream == "" {
			return nil, fmt.Errorf("source %q: empty stream", source.Path)
		}
		if source.Parser == nil {
			return nil, fmt.Errorf("source %q: nil parser", source.Stream)
		}
		if _, exists := c.engines[source.Stream]; exists {
			return nil, fmt.Errorf("duplicate source stream %q", source.Stream)
		}
		engine, err := newEngine(store, states, source.Stream, source.Path, source.Parser, opts...)
		if err != nil {
			return nil, err
		}
		if err := engine.configured(); err != nil {
			return nil, err
		}
		c.engines[source.Stream] = engine
	}
	return c, nil
}

func (c *Collector) Pump(output Output) error {
	var failures []error
	for _, source := range c.sources {
		if err := c.engines[source.Stream].Pump(output); err != nil {
			failures = append(failures, SourceError{Source: source, Err: err})
		}
	}
	return errors.Join(failures...)
}

func (c *Collector) Engine(stream string) *Engine {
	return c.engines[stream]
}

func (c *Collector) Recovery() []RecoveryReport {
	reports := make([]RecoveryReport, 0, len(c.sources))
	for _, source := range c.sources {
		reports = append(reports, c.engines[source.Stream].Recovery())
	}
	return reports
}

func (c *Collector) Close() error {
	var failures []error
	for _, source := range c.sources {
		if err := c.engines[source.Stream].Close(); err != nil {
			failures = append(failures, SourceError{Source: source, Err: err})
		}
	}
	return errors.Join(failures...)
}
