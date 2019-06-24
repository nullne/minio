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
	errReadOnly error = errors.New("file is read only")
)

type file struct {
	id   int32
	path string

	data *os.File
	lock sync.RWMutex
	wg   sync.WaitGroup
	// means not writable
	readOnly bool
	directIO bool
}

const (
	MaxFileSize    int64 = 4 << 30 //4GB
	dataFileSuffix       = ".data"
)

func createFile(dir string, id int32) (f *file, err error) {
	f = new(file)
	f.id = id
	path := filepath.Join(dir, fmt.Sprintf("%d%s", id, dataFileSuffix))
	f.path = path
	f.data, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if err := Fallocate(int(f.data.Fd()), 0, MaxFileSize); err != nil {
		if e := f.remove(); e != nil {
			fmt.Println("failed to remove dangling file", e)
		}
		return nil, err
	}
	return
}

func openFileToRead(p string) (f *file, err error) {
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
	f.id = int32(id)
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
		f, err := openFileToRead(filepath.Join(dir, info.Name()))
		if err != nil {
			return nil, err
		}

		fs = append(fs, f)
	}
	return fs, nil
}

func (f *file) isReadOnly() bool {
	var readOnly bool
	f.lock.RLock()
	readOnly = f.readOnly
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

func (f *file) setReadOnly() {
	f.lock.Lock()
	f.readOnly = true
	f.lock.Unlock()
}

func (f *file) read(buffer []byte, offset int64) (int64, error) {
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "fileVolume-read"}).Observe(time.Since(before).Seconds())
	}(time.Now())
	f.wg.Add(1)
	defer f.wg.Done()

	n, err := f.data.ReadAt(buffer, offset)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return int64(n), err
}

// no concurrent use
func (f *file) write(data []byte) (int64, error) {
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "fileVolume-write"}).Observe(time.Since(before).Seconds())
	}(time.Now())
	f.wg.Add(1)
	defer f.wg.Done()

	if f.isReadOnly() {
		return 0, errReadOnly
	}
	end, err := f.data.Seek(0, io.SeekEnd)
	if err != nil {
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
		return 0, err
	}
	if end+int64(n) >= MaxFileSize {
		f.setReadOnly()
	}
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
