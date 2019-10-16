package volume

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrDBClosed      = errors.New("db has already been closed")
	ErrKeyNotExisted = errors.New("key not existed")
)

type IndexOptions struct {
	Root               string
	BackupRoot         string
	BackupInterval     string
	BackupStartBetween string // 02:00:00-04:00:00
}

type Index interface {
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	Delete(key string) error
	// StatDir(key string) (fi FileInfo, err error)
	ListN(key, leafFile string, count int) ([]string, error)
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

// entry    prefix	 	result
// a/b/c 	            a/
// a                    a
// a        a
// a/b/c  	a           b/
// aa/b/c 	a
// a/b/c 	b
// p1, p2 will never start with '/'
func SubDir(entry, prefix string) string {
	if !strings.HasPrefix(entry, slashSeperator) {
		entry = slashSeperator + entry
	}

	// "" -> ""
	// "prefix" -> "/prefix"
	// "/" -> ""
	// "prefix/" -> "/prefix"
	// "/prefix/" -> "/prefix"
	if prefix != "" && !strings.HasPrefix(prefix, slashSeperator) {
		prefix = slashSeperator + prefix
	}
	if strings.HasSuffix(prefix, slashSeperator) {
		prefix = strings.TrimSuffix(prefix, slashSeperator)
	}

	if !strings.HasPrefix(entry, prefix) {
		return ""
	}
	s := strings.TrimPrefix(entry, prefix)
	if !strings.HasPrefix(s, slashSeperator) {
		return ""
	}
	s = strings.TrimPrefix(s, slashSeperator)

	ss := strings.SplitAfterN(s, slashSeperator, 2)
	return ss[0]

	// 	if p2 == "" {
	// 		goto firstPart
	// 	}
	// 	if !strings.HasSuffix(p2, "/") {
	// 		p2 += "/"
	// 	}
	// 	if !strings.HasPrefix(p1, p2) {
	// 		return ""
	// 	}
	// 	p1 = strings.TrimPrefix(p1, p2)
	// firstPart:
	// 	p1 = strings.TrimPrefix(p1, "/")
	// 	idx := strings.Index(p1, "/")
	// 	if idx == -1 {
	// 		return p1
	// 	}
	// 	return p1[:idx+1]
}
