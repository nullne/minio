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

func mergeFilters(fs ...func(string) bool) func(string) bool {
	return func(key string) bool {
		for _, f := range fs {
			if !f(key) {
				return false
			}
		}
		return true
	}
}

func directIndexStoring(key string) bool {
	return strings.HasSuffix(key, xlJSONFile)
}

// 不知所云
func fileVolumeStoring(key string) bool {
	return !(strings.HasSuffix(key, xlJSONFile) || strings.HasSuffix(key, "/"))
}
