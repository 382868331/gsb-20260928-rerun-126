package output

import "logengine/pkg/parser"

// Output receives parsed records. Publish must return nil only after the
// record is durably handed off (written/acked downstream). The engine
// commits read positions strictly after a successful Publish, so at-least-
// once delivery holds: a crash before Publish replays the record; a crash
// after Publish but before commit replays it once more, never skips it.
type Output interface {
	Publish(rec parser.Record) error
}
