package volume

import (
	"flag"
	"fmt"
	"sync"
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

func TestCreateFile(t *testing.T) {
	dir := *createPath
	for i := 0; i < 100; i++ {
		f, err := createFile(dir, int32(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.remove(); err != nil {
			t.Fatal(err)
		}
	}
}

var createPath = flag.String("create-path", "/tmp/fuck", "")

func TestParallelCreateFile(t *testing.T) {
	var wg sync.WaitGroup
	for j := 0; j < 100; j++ {
		wg.Add(1)
		go func(j int) {
			dir := fmt.Sprintf("%s-%d", *createPath, j)
			defer wg.Done()
			for i := 0; i < 100; i++ {
				f, err := createFile(dir, int32(i))
				if err != nil {
					t.Fatal(err)
				}
				if err := f.remove(); err != nil {
					t.Fatal(err)
				}
			}
		}(j)
	}
	wg.Wait()

}
