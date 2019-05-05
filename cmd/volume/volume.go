package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/bufio.v1"
)

const (
	lockFileName = "LOCK"
)

var (
	ErrLockFileExisted = errors.New("lock file existed")
)

type Volume struct {
	dir string

	index Index

	data *files
	meta *files
}

func NewVolume(dir string) (v *Volume, err error) {
	v = new(Volume)
	v.dir = dir

	// if err := v.createLockFile(); err != nil {
	// 	return nil, err
	// }
	v.data, err = newFiles(context.TODO(), dir)
	if err != nil {
		return nil, err
	}
	v.meta, err = newFiles(context.TODO(), path.Join("/var/minio/meta", dir))
	if err != nil {
		return nil, err
	}

	// if err := v.loadFiles(); err != nil {
	// 	return nil, err
	// }
	//
	// // make sure the volume can be write to
	// if _, err := v.getFileToWrite(); err != nil {
	// 	return nil, err
	// }

	// path := filepath.Join(dir, fmt.Sprintf("%s.data", name))
	// v = new(Volume)
	// v.dataFile, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	// if err != nil {
	// 	if os.IsPermission(err) {
	// 		v.dataFile, err = os.OpenFile(path, os.O_RDONLY, 0)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		v.WriteAble = false
	// 	} else {
	// 		return nil, err
	// 	}
	// }

	v.index, err = newLevelDBIndex(dir)
	if err != nil {
		return nil, err
	}
	// go v.writeWorker(context.TODO())
	return v, nil
}

func likeMeta(key string) bool {
	return strings.HasSuffix(key, ".json")
}

// func (v Volume) createLockFile() error {
// 	p := filepath.Join(v.dir, lockFileName)
// 	_, err := os.Stat(p)
// 	if err == nil {
// 		return ErrLockFileExisted
// 	} else if !os.IsNotExist(err) {
// 		return err
// 	}
//
// 	f, err := os.Create(p)
// 	if err != nil {
// 		return err
// 	}
// 	return f.Close()
// }

// func (v *Volume) loadFiles() error {
// 	fs, err := loadFiles(v.dir)
// 	if err != nil {
// 		return err
// 	}
// 	files := make([]*file, len(fs))
// 	for i, f := range fs {
// 		s := strings.TrimRight(path.Base(f.path), dataFileSuffix)
// 		id, err := strconv.Atoi(s)
// 		if err != nil {
// 			return err
// 		}
// 		if id >= len(files) {
// 			nf := make([]*file, id+1)
// 			copy(nf, files)
// 			files = nf
// 		}
// 		files[id] = fs[i]
// 		if !f.isReadOnly() {
// 			// fmt.Println("load read only")
// 			wf, err := createFile(path.Dir(f.path), uint32(i))
// 			if err != nil {
// 				return err
// 			}
// 			v.writableFile = wf
// 		}
// 	}
// 	v.files = files
// 	return nil
// }
//
// func (v *Volume) generateNextID() uint32 {
// 	v.flock.RLock()
// 	for i, f := range v.files {
// 		if f == nil {
// 			v.flock.RUnlock()
// 			return uint32(i)
// 		}
// 	}
// 	v.flock.RUnlock()
//
// 	// create one slot for next file
// 	v.flock.Lock()
// 	v.files = append(v.files, nil)
// 	v.flock.Unlock()
// 	return uint32(len(v.files) - 1)
// }
//
// func (v *Volume) addFile() (*file, error) {
// 	id := v.generateNextID()
// 	f, err := createFile(v.dir, id)
// 	if err != nil {
// 		return nil, err
// 	}
// 	rf, err := createReadOnlyFile(f.path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	v.flock.Lock()
// 	v.files[id] = rf
// 	v.flock.Unlock()
//
// 	if v.writableFile != nil {
// 		v.writableFile.data.Close()
// 	}
// 	v.writableFile = f
// 	return f, nil
// }
//
// func (v *Volume) writeWorker(ctx context.Context) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case r := <-v.ch:
// 			info, err := v.write(r.data)
// 			r.resp <- response{
// 				info: info,
// 				err:  err,
// 			}
// 		}
// 	}
// }
//
// func (v *Volume) getFileToWrite() (*file, error) {
// 	if v.writableFile != nil && !v.writableFile.isReadOnly() {
// 		return v.writableFile, nil
// 	}
//
// 	// loop existed files
// 	v.flock.RLock()
// 	for _, f := range v.files {
// 		if f == nil {
// 			fmt.Println("impossible")
// 			continue
// 		}
// 		if !f.isReadOnly() {
// 			wf, err := createFile(path.Dir(f.path), f.id)
// 			v.flock.RUnlock()
// 			if err != nil {
// 				return nil, err
// 			}
// 			if v.writableFile != nil {
// 				v.writableFile.data.Close()
// 			}
// 			v.writableFile = wf
// 			return wf, nil
// 		}
// 	}
// 	v.flock.RUnlock()
//
// 	// create file
// 	return v.addFile()
// }
//
// func (v *Volume) getFileToRead(fid uint32) (*file, error) {
// 	v.flock.RLock()
// 	defer v.flock.RUnlock()
// 	if len(v.files) <= int(fid) {
// 		return nil, fmt.Errorf("file volume %d not found", fid)
// 	}
// 	file := v.files[fid]
// 	if file == nil {
// 		return nil, fmt.Errorf("file volume %d not found", fid)
// 	}
// 	return file, nil
// }

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (v *Volume) ReadAll(key string) ([]byte, error) {
	// fmt.Println("read all:", key)
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadAll"}).Observe(time.Since(before).Seconds())
	}(time.Now())

	before := time.Now()
	info, err := v.index.Get(key)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadAll-GetIndex"}).Observe(time.Since(before).Seconds())
	before = time.Now()
	if err != nil {
		return nil, err
	}
	// if strings.HasSuffix(key, "xl.json") {
	// 	return info.Data(), nil
	// }
	var file *file
	if likeMeta(key) {
		file, err = v.meta.getFileToRead(info.volumeID)
	} else {
		fmt.Println("impossible here")
		file, err = v.data.getFileToRead(info.volumeID)
	}

	// file, err := v.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	bs, err := file.read(int64(info.offset), int64(info.size))
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadAll-ReadData"}).Observe(time.Since(before).Seconds())
	return bs, err
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF.
//
// If an EOF happens after reading some but not all the bytes,
// ReadFile returns ErrUnexpectedEOF.
func (v *Volume) ReadFile(key string, offset int64, buffer []byte) (int64, error) {
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadFile"}).Observe(time.Since(before).Seconds())
	}(time.Now())
	before := time.Now()
	info, err := v.index.Get(key)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadFile-GetIndex"}).Observe(time.Since(before).Seconds())
	before = time.Now()
	if err != nil {
		return 0, err
	}
	var file *file
	if likeMeta(key) {
		file, err = v.meta.getFileToRead(info.volumeID)
	} else {
		file, err = v.data.getFileToRead(info.volumeID)
	}
	// file, err := v.getFileToRead(info.volumeID)
	if err != nil {
		return 0, err
	}
	n, err := file.readInto(buffer, int64(info.offset)+offset)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadFile-ReadData"}).Observe(time.Since(before).Seconds())
	return n, err
}

func (v *Volume) ReadFileStream(key string, offset, length int64) (io.ReadCloser, error) {
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadFileStream"}).Observe(time.Since(before).Seconds())
	}(time.Now())

	before := time.Now()
	info, err := v.index.Get(key)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadFileStream-GetIndex"}).Observe(time.Since(before).Seconds())
	before = time.Now()
	if err != nil {
		return nil, err
	}
	var file *file
	if likeMeta(key) {
		file, err = v.meta.getFileToRead(info.volumeID)
	} else {
		file, err = v.data.getFileToRead(info.volumeID)
	}
	// file, err := v.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}

	if offset > int64(info.size) {
		offset = int64(info.size)
		length = 0
	}
	if offset+length > int64(info.size) {
		length = int64(info.size) - offset
	}
	data, err := file.read(int64(info.offset)+offset, length)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "ReadFileStream-ReadData"}).Observe(time.Since(before).Seconds())
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bufio.NewBuffer(data)), nil
}

func (v *Volume) WriteAll(key string, size int64, r io.Reader) error {
	// fmt.Println("write all:", key)
	defer func(before time.Time) {
		DiskOperationDuration.With(prometheus.Labels{"operation_type": "WriteAll"}).Observe(time.Since(before).Seconds())
	}(time.Now())
	before := time.Now()
	// io.Reader isn't predictable
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "WriteAll-ReadAll"}).Observe(time.Since(before).Seconds())
	before = time.Now()

	// if strings.HasSuffix(key, "xl.json") {
	// 	return v.index.Set(key, FileInfo{
	// 		fileName: key,
	// 		data:     bs,
	// 	})
	// }

	if int64(len(bs)) != size {
		return errors.New("size mismatch")
	}

	var fi FileInfo

	if likeMeta(key) {
		fi, err = v.meta.write(bs)
	} else {
		fi, err = v.data.write(bs)
	}

	// req := request{
	// 	data: bs,
	// 	resp: make(chan response),
	// }
	// v.ch <- req
	// resp := <-req.resp
	// if resp.err != nil {
	// 	return resp.err
	// }
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "WriteAll-WriteDate"}).Observe(time.Since(before).Seconds())
	before = time.Now()
	err = v.index.Set(key, fi)
	DiskOperationDuration.With(prometheus.Labels{"operation_type": "WriteAll-WriteIndex"}).Observe(time.Since(before).Seconds())
	return err
}

//
// func (v *Volume) write(data []byte) (FileInfo, error) {
// 	file, err := v.getFileToWrite()
// 	if err != nil {
// 		return FileInfo{}, err
// 	}
// 	offset, err := file.write(data)
// 	if err != nil {
// 		if err == errReadOnly {
// 			fmt.Println(err)
// 		}
// 		return FileInfo{}, err
// 	}
// 	return FileInfo{
// 		volumeID: file.id,
// 		offset:   uint64(offset),
// 		size:     uint64(len(data)),
// 		modTime:  time.Now(),
// 	}, nil
// }

// /a/b/c/
// /a/b/d
func (v *Volume) Delete(path string) error {
	return nil
}

func (v *Volume) List(path string, count int) ([]string, error) {
	return v.index.ListN(path, count)
}

func (v *Volume) Stat(key string) (os.FileInfo, error) {
	return v.index.Get(key)
}

// @TODO fake dir mod time
func (v *Volume) StatDir(p string) (os.FileInfo, error) {
	entries, err := v.index.ListN(p, 1)
	if err != nil {
		return nil, err
	}
	if len(entries) != 1 {
		return nil, os.ErrNotExist
	}
	return FileInfo{
		fileName: entries[0],
		isDir:    true,
		// faked time
		modTime: time.Now(),
	}, nil
}

func (v *Volume) Close() error {
	// v.flock.Lock()
	// defer v.flock.Unlock()
	//
	// for _, f := range v.files {
	// 	if v == nil {
	// 		continue
	// 	}
	// 	if err := f.close(); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }
	v.data.close()
	v.meta.close()

	// os.Remove(filepath.Join(v.dir, lockFileName))
	return v.index.Close()
}
