package volume

import (
	"context"
	"strings"
)

type Index interface {
	Get(key string) (fi FileInfo, err error)
	Set(key string, fi FileInfo) error
	Delete(key string) error
	StatDir(key string) (fi FileInfo, err error)
	ListN(key string, count int) ([]string, error)
	ScanAll(ctx context.Context, filter func(string) bool) (chan FileInfo, chan error)
	Close() error
	Remove() error
}

func isMeta(key string) bool {
	return strings.HasSuffix(key, xlJSONFile)
}

func isData(key string) bool {
	return !isMeta(key) && !strings.HasSuffix(key, "/")
}
