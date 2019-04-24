package volume

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MaxFileSize int64 = 4 * (1 << 30) //64GB
	errReadOnly error = errors.New("file is read only")
)

type file struct {
	id   uint32
	path string

	data *os.File
	lock sync.RWMutex
	wg   sync.WaitGroup
	// means not writable
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
	f.data, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if err := Fallocate(int(f.data.Fd()), 0, MaxFileSize); err != nil {
		f.data.Close()
		return nil, err
	}
	return
}

func createReadOnlyFile(p string) (f *file, err error) {
	name := path.Base(p)
	n := strings.Index(name, dataFileSuffix)
	if n == -1 {
		return nil, fmt.Errorf("%s is not a data file", p)
	}
	id, err := strconv.Atoi(name[:n])
	if err != nil {
		return nil, err
	}
	f = new(file)
	f.id = uint32(id)
	f.path = p
	f.data, err = DirectReadOnlyOpen(p, 0666)
	if err != nil {
		return nil, err
	}

	fi, err := f.data.Stat()
	if err != nil {
		return nil, err
	}
	f.readOnly = fi.Size() > MaxFileSize
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
		f, err := createReadOnlyFile(filepath.Join(dir, info.Name()))
		if err != nil {
			return nil, err
		}

		fs = append(fs, f)
	}
	return fs, nil
}

func (f *file) isReadOnly() bool {
	f.lock.RLock()
	readOnly := f.readOnly
	f.lock.RUnlock()
	if f.readOnly {
		return true
	}
	info, err := f.data.Stat()
	if err != nil {
		return true
	}
	readOnly = info.Size() >= MaxFileSize
	if !readOnly {
		return false
	}
	f.lock.Lock()
	f.readOnly = readOnly
	f.lock.Unlock()
	return true
}

func (f *file) readInto(buffer []byte, offset int64) (int64, error) {
	f.wg.Add(1)
	defer f.wg.Done()
	// n, err := f.data.ReadAt(buffer, offset)
	// if err != nil {
	// 	if err == io.EOF && n != 0 {
	// 		return int64(n), io.ErrUnexpectedEOF
	// 	}
	// 	return int64(n), err
	// }
	// return int64(n), nil

	bs, err := f.read(offset, int64(len(buffer)))
	copy(buffer, bs)
	if err != nil {
		return 0, err
		if err == io.EOF && len(bs) != 0 {
			return int64(len(bs)), io.ErrUnexpectedEOF
		}
	}
	return int64(len(bs)), nil
}

func blockSize(n int64, larger bool) int64 {
	if n%4096 == 0 {
		return n
	}
	m := n / 4096
	if larger {
		return 4096 * (m + 1)
	} else {
		return 4096 * m
	}
}

func (f *file) read(offset, size int64) ([]byte, error) {
	f.wg.Add(1)
	defer f.wg.Done()
	noffset := blockSize(offset, false)
	nsize := blockSize(size+offset-noffset, true)
	lag := offset - noffset

	// fmt.Println(nsize, noffset, size, offset)
	bs := make([]byte, nsize)
	n, err := f.data.ReadAt(bs, noffset)
	// fmt.Println(n, size, lag, string(bs))
	if err != nil {
		if err == io.EOF && int64(n) >= (size+lag) {
			return bs[lag : size+lag], nil
		}
		return nil, err
	}
	return bs[lag : size+lag], nil
}

// return offset where data was written to and error, if any
func (f *file) write(data []byte) (int64, error) {
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "write"}).Observe(time.Since(before).Seconds())
	}(time.Now())
	f.wg.Add(1)
	defer f.wg.Done()

	f.lock.Lock()
	if f.readOnly {
		return 0, errReadOnly
	}
	// not sure whether this operation can be removed
	before := time.Now()
	end, err := f.data.Seek(0, io.SeekEnd)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "seek"}).Observe(time.Since(before).Seconds())
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
	if end+int64(n) >= MaxFileSize {
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
