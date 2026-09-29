package logengine

import (
	"fmt"
	"os"
	"syscall"
)

// FileID identifies a physical inode rather than a potentially reused path.
type FileID struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
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
		Device: uint64(sys.Dev),
		Inode:  uint64(sys.Ino),
	}, nil
}

func (id FileID) String() string {
	return fmt.Sprintf("dev=%d inode=%d", id.Device, id.Inode)
}
