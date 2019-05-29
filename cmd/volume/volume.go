package volume

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"gopkg.in/bufio.v1"
)

const (
	xlJSONFile = "xl.json"
)

type Volume struct {
	index Index
	files *files
}

func NewVolume(ctx context.Context, dir string, index Index) (v *Volume, err error) {
	if index == nil {
		return nil, errors.New("nil index")
	}
	v = new(Volume)
	v.index = index
	v.files, err = newFiles(ctx, dir)
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
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	if int64(len(bs)) != size {
		return errors.New("size mismatch")
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

// /a/b/c/
// /a/b/d
func (v *Volume) Delete(path string) error {
	return v.index.Delete(path)
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
	v.files.close()
	return v.index.Close()
}
