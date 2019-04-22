package volume

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type levelDBIndex struct {
	db *leveldb.DB
}

// level db params
var (
	blockCapacity       = 8
	compactionTableSize = 2
)

func init() {
	defer func() {
		fmt.Printf("set leveldb compaction table size to %vm\n", compactionTableSize)
	}()
	s := os.Getenv("MINIO_LEVELDB_COMPACTION_SIZE")
	if s == "" {
		return
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return
	}
	compactionTableSize = i
}

func init() {
	defer func() {
		fmt.Printf("set leveldb block cache to %vm\n", blockCapacity)
	}()
	s := os.Getenv("MINIO_LEVELDB_BLOCK")
	if s == "" {
		return
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return
	}
	blockCapacity = i
}

func newLevelDBIndex(dir string) (Index, error) {
	path := filepath.Join("/var/minio/leveldb", dir, "index")
	// path := filepath.Join("/tmp/leveldb", dir, "index")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(path, &opt.Options{
		BlockCacheCapacity:  1024 * 1024 * blockCapacity,
		CompactionTableSize: 1024 * 1024 * compactionTableSize,
	})
	if err != nil {
		return nil, err
	}

	// iter := db.NewIterator(nil, nil)
	// for iter.Next() {
	// 	iter.Key()
	// 	iter.Value()
	// }
	// iter.Release()
	// if err := iter.Error(); err != nil {
	// 	return nil, err
	// }

	return &levelDBIndex{db: db}, nil
}

func (l *levelDBIndex) Get(key string) (fi FileInfo, err error) {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil {
		return fi, err
	}
	fi.fileName = key

	if strings.HasSuffix(fi.fileName, "xl.json") {
		fi.data = data
		fi.size = uint64(len(data))
		// fi.modTime = time.Now()
		return
	}
	err = fi.UnmarshalBinary(data)
	return
}

func (l *levelDBIndex) Set(key string, fi FileInfo) error {
	if strings.HasSuffix(key, "xl.json") {
		return l.db.Put([]byte(key), fi.data, nil)
	}
	data := fi.MarshalBinary()
	return l.db.Put([]byte(key), data, nil)
}

func (l *levelDBIndex) Delete(key string) error {
	return l.db.Delete([]byte(key), nil)
}

func (l *levelDBIndex) List(keyPrefix string) ([]string, error) {
	return l.listN(keyPrefix, -1)
}

func (l *levelDBIndex) ListN(keyPrefix string, count int) ([]string, error) {
	return l.listN(keyPrefix, count)
}

// count less than 0  means all
// @TODO may need cache to speed up
func (l *levelDBIndex) listN(keyPrefix string, count int) ([]string, error) {
	iter := l.db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	var entries []string

	for iter.Next() {
		if count == 0 {
			break
		}
		key := string(iter.Key())
		entry := subDir(key, keyPrefix)
		if entry == "" {
			continue
		}
		found := false
		for _, e := range entries {
			if e == entry {
				found = true
				break
			}
		}
		if found {
			continue
		}
		entries = append(entries, entry)
		count--
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

// p1		p2	 	result
// a/b/c 	        a/
// a                a
// a/b/c  	a       b/
// aa/b/c 	a
func subDir(p1, p2 string) string {
	if p2 == "" {
		return firstPart(p1)
	}
	if p1 == p2 {
		return p1
	}
	if p2[len(p2)-1] != '/' {
		p2 += "/"
	}
	if !strings.HasPrefix(p1, p2) {
		return ""
	}
	return firstPart(p1[len(p2):])
}

// p never starts with slash
func firstPart(p string) string {
	idx := strings.Index(p, "/")
	if idx == -1 {
		return p
	}
	return p[:idx+1]
}

func (l *levelDBIndex) Close() error {
	return l.db.Close()
}
