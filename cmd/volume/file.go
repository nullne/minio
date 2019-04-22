package volume

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	MaxFileSize int64 = 64 * (1 << 30) //64GB
	errReadOnly error = errors.New("file is read only")
)

type file struct {
	id   uint32
	path string

	data     *os.File
	lock     sync.RWMutex
	wg       sync.WaitGroup
	readOnly bool
}

const (
	dataFileSuffix = ".data"
)

func createFile(dir string, id uint32) (f *file, err error) {
	f = new(file)
	f.id = id
	path := filepath.Join(dir, fmt.Sprintf("%d%s", id, dataFileSuffix))
	f.path = path
	f.data, err = os.Create(path)
	if err != nil {
		return nil, err
	}
	if err := Fallocate(int(f.data.Fd()), 0, MaxFileSize); err != nil {
		f.data.Close()
		return nil, err
	}
	return
}

func loadFiles(dir string) (fs []*file, err error) {
	defer func() {
		if err != nil {
			for _, f := range fs {
				f.close()
			}
		}
	}()

	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		if !strings.HasSuffix(info.Name(), dataFileSuffix) {
			continue
		}
		id, err := strconv.Atoi(strings.TrimRight(info.Name(), dataFileSuffix))
		if err != nil {
			return nil, err
		}
		fp := filepath.Join(dir, info.Name())
		data, err := os.OpenFile(fp, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		fi, err := data.Stat()
		if err != nil {
			return nil, err
		}
		fs = append(fs, &file{
			id:       uint32(id),
			path:     fp,
			data:     data,
			readOnly: fi.Size() > MaxFileSize,
		})
	}
	return fs, nil
}

func (f *file) isReadOnly() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.readOnly
}

func (f *file) readInto(buffer []byte, offset int64) (int64, error) {
	f.wg.Add(1)
	defer f.wg.Done()
	n, err := f.data.ReadAt(buffer, offset)
	if err != nil {
		if err == io.EOF && n != 0 {
			return int64(n), io.ErrUnexpectedEOF
		}
		return int64(n), err
	}
	return int64(n), nil
}

func (f *file) read(offset, size int64) ([]byte, error) {
	f.wg.Add(1)
	defer f.wg.Done()
	bs := make([]byte, size)
	_, err := f.data.ReadAt(bs, offset)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

// return offset where data was written to and error, if any
func (f *file) write(data []byte) (int64, error) {
	f.wg.Add(1)
	defer f.wg.Done()

	f.lock.Lock()
	if f.readOnly {
		return 0, errReadOnly
	}
	// not sure whether this operation can be removed
	end, err := f.data.Seek(0, io.SeekEnd)
	if err != nil {
		f.lock.Unlock()
		return 0, fmt.Errorf("failed to seek volume to the end: %v", err)
	}

	defer func(w *os.File, off int64) {
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
				fmt.Printf("failed to truncate %s back to %d with error: %v\n", w.Name(), end, te)
			}
		}
	}(f.data, end)
	n, err := f.data.Write(data)
	if err != nil {
		f.lock.Unlock()
		return 0, err
	}
	if end+int64(n) > MaxFileSize {
		f.readOnly = true
	}
	f.lock.Unlock()

	return end, nil
}

func (f *file) remove() error {
	if err := f.close(); err != nil {
		return err
	}
	return os.Remove(f.path)
}

func (f *file) close() error {
	f.wg.Wait()
	if err := f.data.Sync(); err != nil {
		return err
	}
	return f.data.Close()
}
