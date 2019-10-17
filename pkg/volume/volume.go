// volume stores small files into a bigger one, and records the corresponding
// index into rocksdb now which can be replaced by other db. In some cases,
// such that the file is extremely small, we store it directly into rocksdb
// for faster reading
package volume

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/minio/minio/pkg/volume/interfaces"
	"go.uber.org/multierr"

	bf "gopkg.in/bufio.v1"
)

const (
	IndexDir = "index"
	DataDir  = "data"

	slashSeperator = "/"
)

var (
	ErrInvalidArgument = errors.New("invalid argument specified")
)

type Volume struct {
	dir   string
	index Index
	files *files

	// save the data directly in the index db rather than big file
	directIndexSaving func(string) bool
}

var disableDirectIndexSaving = func(string) bool { return false }

func NewVolume(ctx context.Context, dir string, idx Index) (*Volume, error) {
	var err error
	if dir, err = GetValidPath(dir); err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, errors.New("nil index")
	}

	files, err := newFiles(ctx, path.Join(dir, DataDir))
	if err != nil {
		return nil, err
	}
	return &Volume{
		dir:               dir,
		index:             idx,
		files:             files,
		directIndexSaving: disableDirectIndexSaving,
	}, nil
}

// MUST set before volume is working
func (v *Volume) SetDirectIndexSaving(fn func(string) bool) {
	v.directIndexSaving = fn
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (v *Volume) ReadAll(key string) ([]byte, error) {
	if isDirectory(key) {
		return nil, interfaces.ErrIsNotReguler
	}
	bs, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}

	if v.directIndexSaving(key) {
		if len(bs) < len8 {
			return nil, io.ErrUnexpectedEOF
		}
		return bs[len8:], nil
	}

	info, err := UnmarshalFileInfo(key, bs)
	if err != nil {
		return nil, err
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	data := make([]byte, info.size)
	_, err = file.read(data, int64(info.offset))
	return data, err
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF.
//
// If an EOF happens after reading some but not all the bytes,
// ReadFile returns ErrUnexpectedEOF.
func (v *Volume) ReadFile(key string, offset int64, buffer []byte) (int64, error) {
	if isDirectory(key) {
		return 0, interfaces.ErrIsNotReguler
	}
	bs, err := v.index.Get(key)
	if err != nil {
		return 0, err
	}

	if v.directIndexSaving(key) {
		if int64(len(bs)-len8) < +offset {
			return 0, io.ErrUnexpectedEOF
		}
		n := copy(buffer, bs[int64(len8)+offset:])
		return int64(n), nil
	}

	info, err := UnmarshalFileInfo(key, bs)
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
	if isDirectory(key) {
		return nil, interfaces.ErrIsNotReguler
	}
	bs, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}

	if v.directIndexSaving(key) {
		offset += len8
		if int64(len(bs)) < offset {
			return nil, io.ErrUnexpectedEOF
		}
		if offset+length > int64(len(bs)) {
			length = int64(len(bs)) - offset
		}
		return ioutil.NopCloser(bf.NewBuffer(bs[offset : offset+length])), nil
	}

	info, err := UnmarshalFileInfo(key, bs)
	if err != nil {
		return nil, err
	}
	if offset > int64(info.size) {
		return ioutil.NopCloser(bf.NewBuffer([]byte{})), nil
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	if offset+length > int64(info.size) {
		length = int64(info.size) - offset
	}
	data := make([]byte, length)
	_, err = file.read(data, int64(info.offset)+offset)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bf.NewBuffer(data)), nil
}

func (v *Volume) WriteAll(key string, bs []byte) error {
	if v.directIndexSaving(key) {
		info := NewShortFileInfo(key).MarshalBinary()
		return v.index.Set(key, append(info, bs...))
	}

	fi, err := v.files.write(bs)
	if err != nil {
		return err
	}
	return v.index.Set(key, fi.MarshalBinary())
}

// Delete path /data/bucket/object will delete all following keys
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

func (v *Volume) List(p, leafFile string, count int) ([]string, error) {
	return v.index.ListN(p, leafFile, count)
}

func (v *Volume) Stat(key string) (os.FileInfo, error) {
	data, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}
	var fi FileInfo
	if v.directIndexSaving(key) {
		fi = NewShortFileInfo(key)
	} else {
		fi = NewFileInfo(key)
	}
	if err := fi.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return fi, nil
}

// Mkdir is a little bit different from os.Mkdir, but should work well with object storage
func (v *Volume) Mkdir(p string) error {
	if !strings.HasSuffix(p, slashSeperator) {
		p += slashSeperator
	}
	if p == "" {
		return ErrInvalidArgument
	}
	_, err := v.index.Get(p)
	if err == nil {
		return os.ErrExist
	} else if err != interfaces.ErrKeyNotExisted {
		return err
	}

	nfi := NewDirInfo(p)
	return v.index.Set(p, nfi.MarshalBinary())
}

// remove the volume itself including data and index
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
