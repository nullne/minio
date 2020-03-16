package volume

import (
	"testing"
	"time"
)

func TestFileInfo(t *testing.T) {
	now := time.Now()
	fi := FileInfo{
		volumeID: 1,
		offset:   1001,
		size:     2002,
		modTime:  now,
	}

	bs := fi.MarshalBinary()

	var fi2 FileInfo
	if err := fi2.UnmarshalBinary(bs); err != nil {
		t.Error(err)
		return
	}
	if fi2.volumeID != fi.volumeID {
		t.Error("volume ID not equal")
	}

	if fi2.offset != fi.offset {
		t.Error("offset not equal")
	}
	if fi2.modTime.UnixNano() != now.UnixNano() {
		t.Errorf("modTime not equal, got %d, wanna %d", fi2.modTime.UnixNano(), now.UnixNano())
	}
}
