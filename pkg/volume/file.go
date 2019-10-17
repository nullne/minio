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
	"sync/atomic"
)

var (
	errReadOnly    error = errors.New("file is read only")
	errFileClosing error = errors.New("file is closed")
)

type file struct {
	id   int32
	path string

	data *os.File
	lock sync.RWMutex
	wg   sync.WaitGroup

	readOnly bool
	directIO bool

	isClosed uint32
	deleted  uint32
}

const (
	dataFileSuffix = ".data"
)

var (
	MaxFileSize int64 = 4 << 30 //4GB
)

func init() {
	if s := os.Getenv("FILE_VOLUME_MAX_FILE_SIZE"); s != "" {
		i, err := strconv.Atoi(s)
		if err != nil {
			return
		}
		if i > 4<<30 {
			return
		}
		MaxFileSize = int64(i)
	}
}

func createFile(dir string, id int32) (*file, error) {
	p := filepath.Join(dir, fmt.Sprintf("%d%s", id, dataFileSuffix))
	f := file{
		id:   id,
		path: p,
	}

	var err error
	// @TODO test direct IO vs indirect IO
	// f.data, err = disk.OpenFileDirectIO(p, os.O_CREATE|os.O_WRONLY, 0666)
	f.data, err = os.OpenFile(p, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		// logger.LogIf(context.Background(), fmt.Errorf("failed to create file %s to write: %v", p, err))
		return nil, err
	}
	if err := Fallocate(int(f.data.Fd()), 0, MaxFileSize); err != nil {
		f.close()
		// logger.LogIf(context.Background(), f.close())
		return nil, err
	}
	return &f, nil
}

func openFileToRead(p string) (*file, error) {
	name := path.Base(p)
	if !strings.HasSuffix(name, dataFileSuffix) {
		return nil, fmt.Errorf("%s is not a data file", p)
	}
	id, err := strconv.Atoi(strings.TrimSuffix(name, dataFileSuffix))
	if err != nil {
		return nil, err
	}
	f := file{
		id:   int32(id),
		path: p,
	}
	f.data, err = os.Open(p)
	if err != nil {
		return nil, err
	}

	fi, err := f.data.Stat()
	if err != nil {
		f.close()
		return nil, err
	}
	f.readOnly = fi.Size() > MaxFileSize
	return &f, nil
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
	f.setReadOnly()
	return true
}

func (f *file) setReadOnly() {
	f.lock.Lock()
	f.readOnly = true
	f.lock.Unlock()
}

func (f *file) read(buffer []byte, offset int64) (int64, error) {
	f.wg.Add(1)
	defer f.wg.Done()
	if v := atomic.LoadUint32(&f.isClosed); v == 1 {
		return 0, errFileClosing
	}

	n, err := f.data.ReadAt(buffer, offset)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF

		// make sure whether it's not found or really EOF
		fi, e := f.data.Stat()
		if e == nil && fi.Size() < offset+int64(len(buffer)) {
			err = os.ErrNotExist
		}
	}
	return int64(n), err
}

func (f *file) write(data []byte) (int64, error) {
	f.wg.Add(1)
	defer f.wg.Done()
	if v := atomic.LoadUint32(&f.isClosed); v == 1 {
		return 0, errFileClosing
	}

	if f.isReadOnly() {
		return 0, errReadOnly
	}
	end, err := f.data.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	defer func(w *os.File, off int64) {
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// logger.LogIf(context.Background(), fmt.Errorf("failed to truncate %s back to %d with error: %v", w.Name(), end, te))
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
	if err := os.Remove(f.path); err != nil {
		return err
	}

	atomic.StoreUint32(&(f.deleted), 1)
	return nil
}

func (f *file) isDeleted() bool {
	return atomic.LoadUint32(&(f.deleted)) == 1
}

func (f *file) close() error {
	if v := atomic.LoadUint32(&f.isClosed); v == 1 {
		return nil
	}
	atomic.StoreUint32(&f.isClosed, 1)
	f.wg.Wait()
	if err := f.data.Sync(); err != nil {
		atomic.StoreUint32(&f.isClosed, 0)
		return err
	}
	if err := f.data.Close(); err != nil {
		atomic.StoreUint32(&f.isClosed, 0)
		return err
	}
	return nil
}
