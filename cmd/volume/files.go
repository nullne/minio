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
	mu              sync.Mutex   //make sure delete and create file won't happen at the same time
	ch              chan request
	writableFile    *file
	chWritableFile  chan *file
	createFileError error
	createFileLock  sync.RWMutex
	flock           fileLock

	wg     sync.WaitGroup
	done   chan struct{}
	closed uint32
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
	fs.done = make(chan struct{})

	if err := fs.loadFiles(); err != nil {
		return nil, err
	}

	fs.wg.Add(1)
	go fs.prepareFileToWrite()

	// wait the first wriable file
	time.Sleep(time.Millisecond * 10)
	if _, err := fs.getFileToWrite(context.Background()); err != nil {
		logger.LogIf(context.Background(), err)
	}

	fs.wg.Add(1)
	go fs.writeWorker()
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

func (fs *files) prepareFileToWrite() {
	defer fs.wg.Done()
	var cur int32 = -1
	for {
		wr, err := fs.createFile(cur)
		fs.setCreateFileError(err)
		if err != nil {
			if isSysErrNoSpace(err) {
				return
			}
			logger.LogIf(context.Background(), err)
			time.Sleep(10 * time.Second)
			continue
		}

		select {
		case fs.chWritableFile <- wr:
			cur = wr.id
		case <-fs.done:
			wr.close()
			return
		}
	}
}

type request struct {
	data []byte
	resp chan response
}

type response struct {
	info FileInfo
	err  error
}

func (fs *files) writeWorker() {
	defer fs.wg.Done()
	for r := range fs.ch {
		info, err := fs.writeData(context.Background(), r.data)
		r.resp <- response{
			info: info,
			err:  err,
		}
	}
}

func (fs *files) createFile(cur int32) (*file, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

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
		ns := make([]*file, len(files)+1)
		copy(ns, files)
		files = ns
		fid = int32(len(files) - 1)
	}

	wr, err := createFile(fs.dir, fid)
	if err != nil {
		return nil, err
	}

	if files[fid] == nil {
		f, err := openFileToRead(wr.path)
		if err != nil {
			wr.close()
			return nil, err
		}
		files[fid] = f
	}

	fs.files.Store(files)
	return wr, nil
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

func (fs *files) write(data []byte) (FileInfo, error) {
	req := request{
		data: data,
		resp: make(chan response),
	}

	//@TODO change to context later
	timer := time.NewTimer(time.Second * 1)
	defer timer.Stop()
	select {
	case <-timer.C:
		return FileInfo{}, ErrWriteTimeout
	case fs.ch <- req:
	}
	resp := <-req.resp
	return resp.info, resp.err
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

func (fs *files) deleteFile(fid uint32) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	files := fs.files.Load().([]*file)
	if len(files) <= int(fid) {
		return os.ErrNotExist
	}
	f := files[fid]
	if f == nil {
		return os.ErrNotExist
	}
	nfs := make([]*file, len(files))
	for i := range files {
		if i != int(fid) {
			nfs[i] = files[i]
		}
	}
	fs.files.Store(nfs)

	if err := f.remove(); err != nil {
		return err
	}
	return nil
}

// volume invoker gurantees that all requests are gone
func (fs *files) close() error {
	if !atomic.CompareAndSwapUint32(&fs.closed, 0, 1) {
		return nil
	}
	close(fs.done)
	close(fs.ch)
	fs.wg.Wait()
	files := fs.files.Load().([]*file)
	for _, f := range files {
		if f == nil {
			continue
		}
		logger.LogIf(context.Background(), f.close())
	}
	logger.LogIf(context.Background(), fs.writableFile.close())
	select {
	case f := <-fs.chWritableFile:
		if f != nil {
			logger.LogIf(context.Background(), f.close())
		}
	default:
	}
	return fs.flock.release()
}

func (fs *files) remove() error {
	if err := fs.close(); err != nil {
		return err
	}
	return os.RemoveAll(fs.dir)
}

func isSysErrNoSpace(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no space left on device")
	// if err == syscall.ENOSPC {
	// 	return true
	// }
	// pathErr, ok := err.(*os.PathError)
	// return ok && pathErr.Err == syscall.ENOSPC
}
