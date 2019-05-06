package volume

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type levelDBIndex struct {
	db *leveldb.DB
}

// level db params
var (
	levelDBCache = 16
	levelDBRoot  = "/"
)

func init() {
	defer func() {
		fmt.Printf("set leveldb cache usage to %vm\n", levelDBCache)
	}()
	s := os.Getenv("MINIO_LEVELDB_CACHE")
	if s == "" {
		return
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return
	}
	levelDBCache = i
}

func init() {
	defer func() {
		fmt.Printf("set leveldb root to %v\n", levelDBRoot)
	}()
	s := os.Getenv("MINIO_LEVELDB_ROOT")
	if s == "" {
		return
	}
	levelDBRoot = s
}

func newLevelDBIndex(dir string) (Index, error) {
	path := filepath.Join(levelDBRoot, dir, "index")
	// path := filepath.Join("/tmp/leveldb", dir, "index")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(path, &opt.Options{
		// CompactionTableSize: compactionTableSize * opt.MiB,
		BlockCacheCapacity: levelDBCache * opt.MiB / 2,
		WriteBuffer:        levelDBCache * opt.MiB / 4,
		Filter:             filter.NewBloomFilter(10),
	})
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		properties := []string{
			"leveldb.stats",
			"leveldb.iostats",
			"leveldb.writedelay",
			"leveldb.sstables",
			"leveldb.blockpool",
			"leveldb.cachedblock",
			"leveldb.openedtables",
			"leveldb.alivesnaps",
			"leveldb.aliveiters",
		}
		for _ = range ticker.C {
			log.Println("\n\n", path)
			for _, p := range properties {
				value, err := db.GetProperty(p)
				if err != nil {
					continue
				}
				log.Printf("%s: %s \n", p, value)
			}
			log.Println(path, "\n\n")
		}
	}()

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

// func (l *levelDBIndex) stat() {
// 	var s *leveldb.DBStats
// 	if err := l.db.Stats(s); err != nil {
// 		panic(err)
// 	}
// 	// fmt.Println(s.
// }

func (l *levelDBIndex) Get(key string) (fi FileInfo, err error) {
	data, err := l.db.Get([]byte(key), nil)
	if err != nil {
		return fi, err
	}
	fi.fileName = key

	// if strings.HasSuffix(fi.fileName, "xl.json") {
	// 	fi.data = data
	// 	fi.size = uint64(len(data))
	// 	// fi.modTime = time.Now()
	// 	return
	// }
	err = fi.UnmarshalBinary(data)
	return
}

func (l *levelDBIndex) Set(key string, fi FileInfo) error {
	// opt := levigo.NewWriteOptions()
	// if strings.HasSuffix(key, "xl.json") {
	// 	return l.db.Put(opt, []byte(key), fi.data)
	// }
	data := fi.MarshalBinary()
	return l.db.Put([]byte(key), data, nil)
}

func (l *levelDBIndex) Delete(key string) error {
	return nil
	// return l.db.Delete([]byte(key), nil)
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
	return nil, nil
	// iter := l.db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	// var entries []string
	//
	// for iter.Next() {
	// 	if count == 0 {
	// 		break
	// 	}
	// 	key := string(iter.Key())
	// 	entry := subDir(key, keyPrefix)
	// 	if entry == "" {
	// 		continue
	// 	}
	// 	found := false
	// 	for _, e := range entries {
	// 		if e == entry {
	// 			found = true
	// 			break
	// 		}
	// 	}
	// 	if found {
	// 		continue
	// 	}
	// 	entries = append(entries, entry)
	// 	count--
	// }
	// iter.Release()
	// if err := iter.Error(); err != nil {
	// 	return nil, err
	// }
	// return entries, nil
}

func (l *levelDBIndex) Close() error {
	return l.db.Close()
}
