package volume

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	"go.uber.org/multierr"
	"gopkg.in/bufio.v1"
)

const (
	xlJSONFile = "xl.json"

	IndexDir       = "index"
	DataDir        = "data"
	IndexBackupDir = "backup"
)

type Volume struct {
	dir   string
	index Index
	files *files
}

func NewVolume(ctx context.Context, dir string) (v *Volume, err error) {
	index, err := NewRocksDBIndex(dir, parseRocksDBOptionsFromEnv())
	if err != nil {
		return nil, err
	}
	v = new(Volume)
	v.dir = dir
	v.index = index
	v.files, err = newFiles(ctx, path.Join(dir, DataDir))
	if err != nil {
		return nil, err
	}
	return v, nil
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (v *Volume) ReadAll(key string) ([]byte, error) {
	info, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", key)
	}
	if strings.HasSuffix(key, xlJSONFile) {
		return info.Data(), nil
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	bs := make([]byte, info.size)
	_, err = file.read(bs, int64(info.offset))
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
	info, err := v.index.Get(key)
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		return 0, fmt.Errorf("%s is a directory", key)
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return 0, err
	}
	n, err := file.read(buffer, int64(info.offset)+offset)
	return n, err
}

func (v *Volume) ReadFileStream(key string, offset, length int64) (io.ReadCloser, error) {
	info, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", key)
	}
	file, err := v.files.getFileToRead(info.volumeID)
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
	bs := make([]byte, length)
	_, err = file.read(bs, int64(info.offset)+offset)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bufio.NewBuffer(bs)), nil
}

func (v *Volume) WriteAll(key string, size int64, r io.Reader) error {
	// io.Reader isn't predictable
	bs := make([]byte, size)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}

	if strings.HasSuffix(key, xlJSONFile) {
		return v.index.Set(key, FileInfo{
			fileName: key,
			data:     bs,
		})
	}

	fi, err := v.files.write(bs)
	if err != nil {
		return err
	}
	return v.index.Set(key, fi)
}

// delete /data/bucket/object will delete all following keys
// /data1/bucket/object/xl.json
// /data1/bucket/object/part.1
// /data1/bucket/object/part.2
// return error "not empty", if delete /data1/bucket (not the object directory)
func (v *Volume) Delete(path string) error {
	if path == "" {
		return nil
	}
	return v.index.Delete(path)
}

func (v *Volume) List(p string, count int) ([]string, error) {
	return v.index.ListN(p, count)
}

func (v *Volume) Stat(key string) (os.FileInfo, error) {
	return v.index.Get(key)
}

// fake dir mod time
func (v *Volume) StatDir(p string) (os.FileInfo, error) {
	if p == "" {
		return nil, os.ErrNotExist
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return v.index.StatDir(p)
}

func (v *Volume) MakeDir(p string) error {
	if p == "" {
		return nil
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return v.index.Set(p, FileInfo{
		fileName: p,
		isDir:    true,
		modTime:  time.Now(),
	})
}

// remove the volume itself including data and index
// remove the backup dir also
func (v *Volume) Remove() (err error) {
	err = multierr.Append(err, v.index.Remove())
	err = multierr.Append(err, v.files.remove())
	err = multierr.Append(err, os.RemoveAll(v.dir))
	return
}

func (v *Volume) Close() (err error) {
	err = multierr.Append(err, v.index.Close())
	err = multierr.Append(err, v.files.close())
	return
}

// pathJoin - like path.Join() but retains trailing "/" of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if strings.HasSuffix(elem[len(elem)-1], "/") {
			trailingSlash = "/"
		}
	}
	ps := path.Join(elem...)
	if ps == "/" {
		return ps
	}
	return ps + trailingSlash
}

func catchPanic() {
	if err := recover(); err != nil {
		logger.Info("catch panic: %+v", err)
	}
}
