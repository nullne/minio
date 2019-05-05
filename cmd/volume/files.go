package volume

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type files struct {
	dir          string
	files        []*file
	flock        sync.RWMutex
	createLock   sync.Mutex
	ch           chan request
	writableFile *file
}

// data and meta can save on different place
func newFiles(ctx context.Context, dir string) (*files, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	fs := files{
		dir: dir,
		ch:  make(chan request),
	}

	if err := fs.loadFiles(); err != nil {
		return nil, err
	}

	// make sure the volume can be write to
	if _, err := fs.getFileToWrite(); err != nil {
		return nil, err
	}

	go fs.writeWorker(ctx)
	return &fs, nil
}

func (fis *files) loadFiles() error {
	fs, err := loadFiles(fis.dir)
	if err != nil {
		return err
	}
	files := make([]*file, len(fs))
	for i, f := range fs {
		s := strings.TrimRight(path.Base(f.path), dataFileSuffix)
		id, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		if id >= len(files) {
			nf := make([]*file, id+1)
			copy(nf, files)
			files = nf
		}
		files[id] = fs[i]
		if !f.isReadOnly() {
			// fmt.Println("load read only")
			wf, err := createFile(path.Dir(f.path), uint32(i))
			if err != nil {
				return err
			}
			fis.writableFile = wf
		}
	}
	fis.files = files
	return nil
}

func (fs *files) generateNextID() uint32 {
	fs.flock.RLock()
	for i, f := range fs.files {
		if f == nil {
			fs.flock.RUnlock()
			return uint32(i)
		}
	}
	fs.flock.RUnlock()

	// create one slot for next file
	fs.flock.Lock()
	fs.files = append(fs.files, nil)
	fs.flock.Unlock()
	return uint32(len(fs.files) - 1)
}

func (fs *files) addFile() (*file, error) {
	id := fs.generateNextID()
	f, err := createFile(fs.dir, id)
	if err != nil {
		return nil, err
	}
	rf, err := createReadOnlyFile(f.path)
	if err != nil {
		return nil, err
	}
	fs.flock.Lock()
	fs.files[id] = rf
	fs.flock.Unlock()

	if fs.writableFile != nil {
		fs.writableFile.data.Close()
	}
	fs.writableFile = f
	return f, nil
}

type request struct {
	data []byte
	resp chan response
}

type response struct {
	info FileInfo
	err  error
}

func (fs *files) getFileToWrite() (*file, error) {
	if fs.writableFile != nil && !fs.writableFile.isReadOnly() {
		return fs.writableFile, nil
	}

	// loop existed files
	fs.flock.RLock()
	for _, f := range fs.files {
		if f == nil {
			fmt.Println("impossible")
			continue
		}
		if !f.isReadOnly() {
			wf, err := createFile(path.Dir(f.path), f.id)
			fs.flock.RUnlock()
			if err != nil {
				return nil, err
			}
			if fs.writableFile != nil {
				fs.writableFile.data.Close()
			}
			fs.writableFile = wf
			return wf, nil
		}
	}
	fs.flock.RUnlock()

	// create file
	return fs.addFile()
}

func (fs *files) getFileToRead(fid uint32) (*file, error) {
	fs.flock.RLock()
	defer fs.flock.RUnlock()
	if len(fs.files) <= int(fid) {
		return nil, fmt.Errorf("file volume %d not found", fid)
	}
	file := fs.files[fid]
	if file == nil {
		return nil, fmt.Errorf("file volume %d not found", fid)
	}
	return file, nil
}

func (fs *files) write(data []byte) (FileInfo, error) {
	req := request{
		data: data,
		resp: make(chan response),
	}
	fs.ch <- req
	resp := <-req.resp
	return resp.info, resp.err
}

func (fs *files) writeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-fs.ch:
			info, err := fs.writeData(r.data)
			r.resp <- response{
				info: info,
				err:  err,
			}
		}
	}
}

func (fs *files) writeData(data []byte) (FileInfo, error) {
	file, err := fs.getFileToWrite()
	if err != nil {
		return FileInfo{}, err
	}
	offset, err := file.write(data)
	if err != nil {
		if err == errReadOnly {
			fmt.Println(err)
		}
		return FileInfo{}, err
	}
	return FileInfo{
		volumeID: file.id,
		offset:   uint64(offset),
		size:     uint64(len(data)),
		modTime:  time.Now(),
	}, nil
}

func (fs *files) close() error {
	fs.flock.Lock()
	defer fs.flock.Unlock()
	for _, f := range fs.files {
		if f == nil {
			continue
		}
		if err := f.close(); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}
