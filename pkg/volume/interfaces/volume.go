package interfaces

import (
	"context"
	"io"
	"os"
)

type Volume interface {
	ReadAll(key string) ([]byte, error)
	ReadFile(key string, offset int64, buffer []byte) (int64, error)
	WriteAll(key string, size int64, r io.Reader) error
	Delete(path string) error
	List(p string, count int) ([]string, error)
	Stat(key string) (os.FileInfo, error)
	StatDir(p string) (os.FileInfo, error)
	MakeDir(p string) error
	Maintain(ctx context.Context) error
	DumpListToMaintain(ctx context.Context, rate float64) error
	CleanMaintain() error
	Remove() error
	Close() error
}
