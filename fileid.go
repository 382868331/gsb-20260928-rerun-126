package logengine

import (
	"fmt"
	"os"
	"syscall"
)

// FileID identifies one physical file incarnation rather than a reused path.
type FileID struct {
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	ChangeNanos uint64 `json:"change_nanos,omitempty"`
}

func fileIDFromFile(f *os.File) (FileID, error) {
	st, err := f.Stat()
	if err != nil {
		return FileID{}, err
	}
	return fileIDFromInfo(st)
}

func fileIDFromPath(path string) (FileID, os.FileInfo, error) {
	st, err := os.Stat(path)
	if err != nil {
		return FileID{}, nil, err
	}
	id, err := fileIDFromInfo(st)
	return id, st, err
}

func fileIDFromInfo(st os.FileInfo) (FileID, error) {
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return FileID{}, fmt.Errorf("unsupported file information type %T", st.Sys())
	}
	return FileID{
		Device:      uint64(sys.Dev),
		Inode:       uint64(sys.Ino),
		ChangeNanos: uint64(sys.Ctim.Nano()),
	}, nil
}

func (id FileID) String() string {
	return fmt.Sprintf("dev=%d inode=%d change=%d", id.Device, id.Inode, id.ChangeNanos)
}

// SameFile reports whether two identities refer to the same device/inode.
// Metadata-time changes on that inode are handled as truncation/reset checks,
// not as path rotation.
func (id FileID) SameFile(other FileID) bool {
	return id.Device == other.Device && id.Inode == other.Inode
}
