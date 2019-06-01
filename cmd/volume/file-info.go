package volume

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type FileInfo struct {
	fileName string
	volumeID uint32
	offset   uint32
	size     uint32
	isDir    bool
	modTime  time.Time
	// properties properties

	data []byte
}

func (f FileInfo) String() string {
	return fmt.Sprintf("%s at volume %v, offset %v, size %v", f.fileName, f.volumeID, f.offset, f.size)
}

func (f FileInfo) Data() []byte {
	return f.data
}

// trim the right slash if possible
func (f FileInfo) Name() string {
	if f.fileName == "/" {
		return f.fileName
	}
	return strings.TrimRight(f.fileName, "/")
}

func (f FileInfo) Size() int64 {
	return int64(f.size)
}

func (f FileInfo) Mode() os.FileMode {
	return 0600
}

func (f FileInfo) ModTime() time.Time {
	return f.modTime
}

func (f FileInfo) IsDir() bool {
	if f.isDir {
		return true
	}
	return f.fileName[len(f.fileName)-1] == '/'
}

func (f FileInfo) Sys() interface{} {
	return nil
}

func (f FileInfo) MarshalBinary() []byte {
	data := make([]byte, 20)
	binary.BigEndian.PutUint32(data[0:4], f.volumeID)
	binary.BigEndian.PutUint32(data[4:8], f.offset)
	binary.BigEndian.PutUint32(data[8:12], f.size)
	binary.BigEndian.PutUint64(data[12:20], uint64(f.modTime.UnixNano()))
	return data
}

func (f *FileInfo) UnmarshalBinary(data []byte) error {
	if len(data) != 20 {
		return errors.New("invalid length")
	}
	f.volumeID = binary.BigEndian.Uint32(data[0:4])
	f.offset = binary.BigEndian.Uint32(data[4:8])
	f.size = binary.BigEndian.Uint32(data[8:12])
	f.modTime = time.Unix(0, int64(binary.BigEndian.Uint64(data[12:20])))
	return nil
}
