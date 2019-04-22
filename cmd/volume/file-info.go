package volume

import (
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"time"
)

type FileInfo struct {
	fileName     string
	volumeID     uint32
	offset, size uint64
	isDir        bool
	modTime      time.Time
	properties   properties

	data []byte
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
	data := make([]byte, 32)
	binary.BigEndian.PutUint64(data[0:8], f.offset)
	binary.BigEndian.PutUint64(data[8:16], f.size)
	binary.BigEndian.PutUint64(data[16:24], uint64(f.modTime.UnixNano()))
	binary.BigEndian.PutUint32(data[24:28], f.volumeID)
	binary.BigEndian.PutUint32(data[28:32], f.properties.Marshal())

	return data
}

func (f *FileInfo) UnmarshalBinary(data []byte) error {
	if len(data) != 32 {
		return errors.New("invalid length")
	}
	f.offset = binary.BigEndian.Uint64(data[0:8])
	f.size = binary.BigEndian.Uint64(data[8:16])
	f.modTime = time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24])))
	f.volumeID = binary.BigEndian.Uint32(data[24:28])
	f.properties.Unmarshal(binary.BigEndian.Uint32(data[28:32]))
	return nil
}

type properties struct {
	deleted bool
}

func (p properties) Marshal() uint32 {
	var ans uint32
	if p.deleted {
		ans |= (1 << 0)
	}
	return ans
}

func (p *properties) Unmarshal(data uint32) {
	val := data & (1 << 0)
	p.deleted = (val > 0)
}
