package logengine

import (
	"encoding/json"
	"fmt"
)

func persistEvents(events []Event) []PersistedEvent {
	out := make([]PersistedEvent, 0, len(events))
	for _, event := range events {
		switch v := event.(type) {
		case Record:
			out = append(out, PersistedEvent{Kind: "record", ID: v.ID, Position: v.Position, Raw: v.Raw, Fields: v.Fields})
		case BadRecord:
			out = append(out, PersistedEvent{Kind: "bad_record", ID: v.ID, Position: v.Position, Reason: v.Reason})
		}
	}
	return out
}

func restoreEvents(persisted []PersistedEvent) []Event {
	out := make([]Event, 0, len(persisted))
	for _, event := range persisted {
		switch event.Kind {
		case "record":
			out = append(out, Record{ID: event.ID, Position: event.Position, Raw: append([]byte(nil), event.Raw...), Fields: event.Fields})
		case "bad_record":
			out = append(out, BadRecord{ID: event.ID, Position: event.Position, Reason: event.Reason})
		}
	}
	return out
}

func parserStateForCheckpoint(parser StreamingParser) (json.RawMessage, error) {
	sp, ok := parser.(StatefulParser)
	if !ok {
		return nil, nil
	}
	return jsonMarshal(sp.Capture())
}

func committedCheckpointFromPending(cp Checkpoint) Checkpoint {
	pending := cp.Pending
	return Checkpoint{
		FileID:      pending.FileID,
		Offset:      pending.Offset,
		NextLine:    pending.NextLine,
		ParserState: pending.ParserState,
	}
}

func restoreMultilineState(parser StatefulParser, raw json.RawMessage) error {
	if _, ok := parser.(*MultilineParser); !ok {
		return nil
	}
	var state multilineState
	if err := jsonUnmarshal(raw, &state); err != nil {
		return fmt.Errorf("restore parser state: %w", err)
	}
	parser.Restore(state)
	return nil
}
