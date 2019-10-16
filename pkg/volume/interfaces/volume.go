package interfaces

import (
	"context"
	"errors"
	"io"
	"os"
)

var (
	ErrDBClosed      = errors.New("db has already been closed")
	ErrNotExisted    = errors.New("not existed")
	ErrKeyNotExisted = errors.New("key not existed")
	ErrIsNotReguler  = errors.New("not of regular file type")
)

type Volume interface {
	ReadAll(key string) ([]byte, error)
	ReadFile(key string, offset int64, buffer []byte) (int64, error)
	ReadFileStream(key string, offset, length int64) (io.ReadCloser, error)
	WriteAll(key string, data []byte) error
	Delete(path string) error
	List(p, leafFile string, count int) ([]string, error)
	Stat(key string) (os.FileInfo, error)
	Mkdir(p string) error
	Maintain(ctx context.Context) error
	DumpListToMaintain(ctx context.Context, rate float64) error
	CleanMaintain() error
	Remove() error
	Close() error
}

type Volumes interface {
	Add(ctx context.Context, p string) error
	Get(p string) (Volume, error)
	Remove(p string) error
	Close() error
}
