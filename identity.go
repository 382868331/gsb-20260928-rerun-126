package logengine

import "fmt"

// stableRecordID builds the identity receivers deduplicate on.
//
// The ID is deterministic in (stream, physical file identity, byte range):
// republishing after a lost acknowledgement or a failed checkpoint write
// produces the same ID, while a different file (rotation) or a different
// offset produces a different ID. Record content is deliberately not part
// of the ID and never appears in it, so secrets cannot leak through it.
func stableRecordID(stream string, fileID FileID, start, end int64) string {
	return fmt.Sprintf("%s@%d:%d:%d-%d", stream, fileID.Device, fileID.Inode, start, end)
}
