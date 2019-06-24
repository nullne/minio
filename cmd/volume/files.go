package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	lockFileName = "LOCK"
)

type fileLock interface {
	release() error
}

type files struct {
	dir             string
	files           atomic.Value // []*file
	ch              chan request
	writableFile    *file
	chWritableFile  chan *file
	createFileError atomic.Value // error to create new file
	flock           fileLock
}

func newFiles(ctx context.Context, dir string) (fs *files, err error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	fl, err := newFileLock(path.Join(dir, lockFileName), false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			fl.release()
		}
	}()

	fs = new(files)
	fs.dir = dir
	fs.ch = make(chan request)
	fs.chWritableFile = make(chan *file)
	fs.flock = fl

	if err := fs.loadFiles(); err != nil {
		return nil, err
	}

	go fs.prepareFileToWrite(ctx)

	// wait the first wriable file
	if _, err := fs.getFileToWrite(ctx); err != nil {
		fmt.Println("read only files")
	}

	go fs.writeWorker(ctx)
	return fs, nil
}

func (fis *files) loadFiles() (err error) {
	fs, err := loadFiles(fis.dir)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			for _, f := range fs {
				f.close()
			}
		}
	}()

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
	}
	fis.files.Store(files)
	return nil
}

type request struct {
	data []byte
	resp chan response
}

type response struct {
	info FileInfo
	err  error
}

func (fs *files) prepareFileToWrite(ctx context.Context) {
	defer close(fs.chWritableFile)
	var cur int32 = -1
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		files := fs.files.Load().([]*file)
		var fid int32 = -1
		// loop existed files
		for i, f := range files {
			if (f == nil || !f.isReadOnly()) && int32(i) != cur {
				fid = int32(i)
				break
			}
		}
		if fid < 0 {
			fid = fs.generateNextID()
			files = fs.files.Load().([]*file)
		}

		wr, err := createFile(fs.dir, fid)
		if err != nil {
			fmt.Println(err)
			fs.createFileError.Store(err.Error())
			// if the error is no space, no need to retry
			if isSysErrNoSpace(err) {
				return
			}
			time.Sleep(10 * time.Second)
			continue
		}

		if files[fid] == nil {
			f, err := openFileToRead(wr.path)
			if err != nil {
				fmt.Println(err)
				fs.createFileError.Store(err.Error())
				time.Sleep(10 * time.Second)
				continue
			}
			files[fid] = f
		}

		fs.files.Store(files)
		fs.createFileError.Store("")

		select {
		case fs.chWritableFile <- wr:
			cur = wr.id
		case <-ctx.Done():
			wr.close()
			return
		}
	}
}

func (fs *files) generateNextID() int32 {
	files := fs.files.Load().([]*file)
	for i, f := range files {
		if f == nil {
			return int32(i)
		}
	}

	// create one slot for next file
	files = append(files, nil)
	fs.files.Store(files)
	return int32(len(files) - 1)
}

func (fs *files) getFileToWrite(ctx context.Context) (*file, error) {
	if fs.writableFile != nil && !fs.writableFile.isReadOnly() {
		return fs.writableFile, nil
	}
retry:
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case f, ok := <-fs.chWritableFile:
		if !ok {
			return nil, errors.New("cannot get file to write")
		}
		if wf := fs.writableFile; wf != nil {
			if err := wf.close(); err != nil {
				fmt.Println(err)
			}
		}
		fs.writableFile = f
	default:
		err := fs.createFileError.Load()
		if err == nil || err == "" {
			time.Sleep(time.Second)
			goto retry
		} else {
			return nil, fmt.Errorf("cannot get file to write: %s", err.(string))
		}
	}
	return fs.writableFile, nil
}

func (fs *files) getFileToRead(fid uint32) (*file, error) {
	files := fs.files.Load().([]*file)
	if len(files) <= int(fid) {
		return nil, fmt.Errorf("file volume %d not found", fid)
	}
	file := files[fid]
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
			info, err := fs.writeData(ctx, r.data)
			r.resp <- response{
				info: info,
				err:  err,
			}
		}
	}
}

func (fs *files) writeData(ctx context.Context, data []byte) (FileInfo, error) {
	file, err := fs.getFileToWrite(ctx)
	if err != nil {
		return FileInfo{}, err
	}
	offset, err := file.write(data)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		volumeID: uint32(file.id),
		offset:   uint32(offset),
		size:     uint32(len(data)),
		modTime:  time.Now(),
	}, nil
}

func (fs *files) close() error {
	files := fs.files.Load().([]*file)
	for _, f := range files {
		if f == nil {
			continue
		}
		if err := f.close(); err != nil {
			fmt.Println(err)
		}
	}
	return fs.flock.release()
}

func (fs *files) remove() error {
	if err := fs.close(); err != nil {
		return err
	}
	return os.RemoveAll(fs.dir)
}

// No space left on device error
func isSysErrNoSpace(err error) bool {
	if err == syscall.ENOSPC {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.ENOSPC
}
