package logengine

import "os"

const (
	RecoveryNew               = "new"
	RecoveryConfirmed         = "confirmed"
	RecoveryPossibleDuplicate = "possible_duplicate"
	RecoveryRotated           = "rotated"
	RecoveryTruncated         = "truncated"
	RecoveryUnavailable       = "unavailable"
)

// ByteRange is a half-open byte interval in one physical file.
type ByteRange struct {
	FileID FileID `json:"file_id"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

// RecoveryReport separates receiver-confirmed progress from bytes that may be
// redelivered because a successful publish was not followed by a durable checkpoint.
type RecoveryReport struct {
	Stream            string     `json:"stream"`
	Path              string     `json:"path"`
	Status            string     `json:"status"`
	ConfirmedFileID   FileID     `json:"confirmed_file_id"`
	ConfirmedOffset   int64      `json:"confirmed_offset"`
	ConfirmedSize     int64      `json:"confirmed_size"`
	LastRecordID      string     `json:"last_record_id,omitempty"`
	PossibleDuplicate *ByteRange `json:"possible_duplicate,omitempty"`
	CurrentFileID     FileID     `json:"current_file_id,omitempty"`
	CurrentSize       int64      `json:"current_size,omitempty"`
	UnreadFromStart   int64      `json:"unread_from_start,omitempty"`
	UnknownOldTail    bool       `json:"unknown_old_tail,omitempty"`
	Readable          bool       `json:"readable"`
	Err               string     `json:"error,omitempty"`
}

func (e *Engine) Recovery() RecoveryReport {
	cp := e.states[e.stream]
	confirmedID := cp.FileID
	if confirmedID.Inode == 0 && cp.LastConfirmedFileID.Inode != 0 {
		confirmedID = cp.LastConfirmedFileID
	}
	report := RecoveryReport{
		Stream:          e.stream,
		Path:            e.path,
		ConfirmedFileID: confirmedID,
		ConfirmedOffset: cp.Offset,
		ConfirmedSize:   cp.Size,
		LastRecordID:    cp.LastRecordID,
		Readable:        true,
	}
	if cp.FileID.Inode == 0 && cp.Offset == 0 && cp.LastConfirmedFileID.Inode == 0 {
		report.Status = RecoveryNew
	}

	id, info, err := fileIDFromPath(e.path)
	if err != nil && !os.IsNotExist(err) {
		report.Status = RecoveryUnavailable
		report.Readable = false
		report.Err = err.Error()
		return report
	}
	if err == nil {
		report.CurrentFileID = id
		report.CurrentSize = info.Size()
	}
	if cp.FileID.Inode == 0 && cp.LastConfirmedFileID.Inode == 0 {
		return report
	}

	if cp.FileID.Inode == 0 {
		report.Status = RecoveryRotated
		report.UnreadFromStart = report.CurrentSize
		return report
	}

	if err == nil && samePhysicalFile(id, cp.FileID) {
		if info.Size() < cp.Offset {
			report.Status = RecoveryTruncated
			report.UnreadFromStart = 0
			return report
		}
		if info.Size() > cp.Offset {
			report.Status = RecoveryPossibleDuplicate
			report.PossibleDuplicate = &ByteRange{FileID: cp.FileID, Start: cp.Offset, End: info.Size()}
		} else {
			report.Status = RecoveryConfirmed
		}
		return report
	}

	report.Status = RecoveryRotated
	if oldPath, found, locateErr := e.findFileByID(cp.FileID); locateErr == nil && found {
		if oldInfo, statErr := os.Stat(oldPath); statErr == nil && oldInfo.Size() > cp.Offset {
			report.Status = RecoveryPossibleDuplicate
			report.PossibleDuplicate = &ByteRange{FileID: cp.FileID, Start: cp.Offset, End: oldInfo.Size()}
		}
	} else {
		report.UnknownOldTail = err != nil || locateErr != nil
	}
	if os.IsNotExist(err) {
		report.UnknownOldTail = report.UnknownOldTail || report.PossibleDuplicate == nil
	}
	return report
}
