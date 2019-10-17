package volume

import (
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"time"
)

type FileInfo struct {
	name    string
	modTime time.Time

	// the following three numbers show how to read itself from big file
	volumeID uint32
	offset   uint32
	size     uint32

	short bool // if true, FileInfo only inclues modTime
}

func NewFileInfo(name string) FileInfo {
	return FileInfo{
		name:    name,
		modTime: time.Now(),
	}
}

// directory must end with slash
func NewDirInfo(name string) FileInfo {
	if !strings.HasSuffix(name, slashSeperator) {
		name += slashSeperator
	}
	fi := NewFileInfo(name)
	return fi
}

func NewShortFileInfo(name string) FileInfo {
	fi := NewFileInfo(name)
	fi.short = true
	return fi
}

// func NewDemoFileInfo(size int) FileInfo {
// 	data := make([]byte, size)
// 	rand.Read(data)
// 	return FileInfo{
// 		data: data,
// 	}
// }
//
// func (f FileInfo) String() string {
// 	return fmt.Sprintf("%s at volume %v, offset %v, size %v", f.name, f.volumeID, f.offset, f.size)
// }

// trim the right slash if possible
func (f FileInfo) Name() string {
	if f.name == slashSeperator {
		return f.name
	}
	return strings.TrimRight(f.name, slashSeperator)
}

func (f FileInfo) Size() int64 {
	return int64(f.size)
}

// faked file mode
func (f FileInfo) Mode() os.FileMode {
	if f.IsDir() {
		return 0777
	}
	return 0666
}

func (f FileInfo) ModTime() time.Time {
	return f.modTime
}

func (f FileInfo) IsDir() bool {
	return strings.HasSuffix(f.name, slashSeperator)
}

func (f FileInfo) Sys() interface{} {
	return nil
}

const (
	len8  = 8
	len20 = 20
)

func (f FileInfo) MarshalBinary() (data []byte) {
	if f.IsDir() || f.short {
		data = make([]byte, len8)
		binary.BigEndian.PutUint64(data[0:8], uint64(f.modTime.UnixNano()))
	} else {
		data = make([]byte, len20)
		binary.BigEndian.PutUint32(data[0:4], f.volumeID)
		binary.BigEndian.PutUint32(data[4:8], f.offset)
		binary.BigEndian.PutUint32(data[8:12], f.size)
		binary.BigEndian.PutUint64(data[12:20], uint64(f.modTime.UnixNano()))
	}
	return
}

func (f *FileInfo) UnmarshalBinary(data []byte) error {
	if f.short {
		if len(data) < len8 {
			return errors.New("invalid length")
		}
		f.modTime = time.Unix(0, int64(binary.BigEndian.Uint64(data[0:8])))
		f.size = uint32(len(data) - len8)
		return nil
	}

	switch len(data) {
	case len8:
		f.modTime = time.Unix(0, int64(binary.BigEndian.Uint64(data[0:8])))
	case len20:
		f.volumeID = binary.BigEndian.Uint32(data[0:4])
		f.offset = binary.BigEndian.Uint32(data[4:8])
		f.size = binary.BigEndian.Uint32(data[8:12])
		f.modTime = time.Unix(0, int64(binary.BigEndian.Uint64(data[12:20])))
	default:
		return errors.New("invalid length")
	}
	return nil
}

func UnmarshalFileInfo(key string, data []byte) (FileInfo, error) {
	fi := FileInfo{name: key}
	err := fi.UnmarshalBinary(data)
	return fi, err
}

// func (fi *FileInfo) Set(data []byte, size uint32) {
// 	fi.data = data
// 	fi.size = size
// }
