package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/cmd/logger"
)

const (
	lockFileName = "LOCK"
)

var (
	ErrWriteTimeout = errors.New("write file timeout")
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
	createFileError error
	createFileLock  sync.RWMutex
	flock           fileLock
}

func (f *files) setCreateFileError(e error) {
	f.createFileLock.Lock()
	defer f.createFileLock.Unlock()
	f.createFileError = e
}

func (f *files) getCreateFileError() error {
	f.createFileLock.RLock()
	defer f.createFileLock.RUnlock()
	return f.createFileError
}

func newFiles(ctx context.Context, dir string) (*files, error) {
	if err := MkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	fl, err := newFileLock(path.Join(dir, lockFileName), false)
	if err != nil {
		return nil, err
	}

	fs := files{
		dir:            dir,
		ch:             make(chan request),
		chWritableFile: make(chan *file),
		flock:          fl,
	}

	if err := fs.loadFiles(); err != nil {
		fl.release()
		return nil, err
	}

	go fs.prepareFileToWrite(ctx)

	// test if writer works
	if _, err := fs.getFileToWrite(ctx); err != nil {
		fs.close()
		return nil, err
	}

	go fs.writeWorker(ctx)
	return &fs, nil
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
	// defer close(fs.chWritableFile)
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
			if f != nil {
				if v := atomic.LoadUint32(&(f.deleted)); v == 1 {
					files[i] = nil
					f = nil
				}
			}
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
			logger.LogIf(ctx, err)
			fs.setCreateFileError(err)
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
				logger.LogIf(ctx, err)
				fs.setCreateFileError(err)
				time.Sleep(10 * time.Second)
				continue
			}
			files[fid] = f
		}

		fs.files.Store(files)
		fs.setCreateFileError(nil)

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
	sleepDuration := time.Millisecond * 10
retry:
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case f := <-fs.chWritableFile:
		if wf := fs.writableFile; wf != nil {
			logger.LogIf(ctx, wf.close())
		}
		fs.writableFile = f
	default:
		err := fs.getCreateFileError()
		if err == nil {
			if sleepDuration > time.Second*30 {
				return nil, errors.New("wait more than 30s and cannot get file to write")
			}
			logger.Info("sleep %s to wait the file to write to be created", sleepDuration.String())
			time.Sleep(sleepDuration)
			sleepDuration *= 2
			goto retry
		} else {
			logger.LogIf(ctx, fmt.Errorf("cannot get file to write: %s", err.Error()))
			return nil, err
		}
	}
	return fs.writableFile, nil
}

func (fs *files) getFileToRead(fid uint32) (*file, error) {
	files := fs.files.Load().([]*file)
	if len(files) <= int(fid) {
		// return nil, fmt.Errorf("file volume %d not found", fid)
		return nil, os.ErrNotExist
	}
	file := files[fid]
	if file == nil {
		// return nil, fmt.Errorf("file volume %d not found", fid)
		return nil, os.ErrNotExist
	}
	return file, nil
}

func (fs *files) write(data []byte) (FileInfo, error) {
	req := request{
		data: data,
		resp: make(chan response),
	}

	//@TODO change to context later
	timer := time.NewTimer(time.Second * 10)
	defer timer.Stop()
	select {
	case <-timer.C:
		return FileInfo{}, ErrWriteTimeout
	case fs.ch <- req:
	}
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

func (fs *files) readOnlyFiles() (fids []int32) {
	files := fs.files.Load().([]*file)
	for _, f := range files {
		if f == nil {
			continue
		}
		if !f.isReadOnly() {
			continue
		}
		fids = append(fids, f.id)
	}
	return
}

func (fs *files) close() error {
	files := fs.files.Load().([]*file)
	for _, f := range files {
		if f == nil {
			continue
		}
		logger.LogIf(context.Background(), f.close())
	}
	return fs.flock.release()
}

func (fs *files) remove() error {
	if err := fs.close(); err != nil {
		return err
	}
	return os.RemoveAll(fs.dir)
}

func (fs *files) deleteFile(fid uint32) error {
	files := fs.files.Load().([]*file)
	if len(files) <= int(fid) {
		return os.ErrNotExist
	}
	file := files[fid]
	if file == nil {
		return os.ErrNotExist
	}
	if err := file.remove(); err != nil {
		return err
	}
	return nil
}

func isSysErrNoSpace(err error) bool {
	return strings.Contains(err.Error(), "no space left on device")
	// if err == syscall.ENOSPC {
	// 	return true
	// }
	// pathErr, ok := err.(*os.PathError)
	// return ok && pathErr.Err == syscall.ENOSPC
}
