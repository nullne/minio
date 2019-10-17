package volume

import (
	"errors"
	"strings"
)

var (
	ErrDBClosed      = errors.New("db has already been closed")
	ErrKeyNotExisted = errors.New("key not existed")
)

type IndexOptions struct {
	Root       string
	BackupRoot string
}

type Index interface {
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	Delete(key string) error
	ListN(key, leafFile string, count int) ([]string, error)
	Close() error
	Remove() error
}

// entry    prefix	 	result
// a/b/c 	            a/
// a                    a
// a        a
// a/b/c  	a           b/
// aa/b/c 	a
// a/b/c 	b
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
}
