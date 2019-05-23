package volume

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tecbot/gorocksdb"
)

type rocksDBIndex struct {
	db *gorocksdb.DB
	wo *gorocksdb.WriteOptions
	ro *gorocksdb.ReadOptions
}

var (
	rocksdbRoot = "/var/minio/rocksdb"
	blockCache  = 64
	directRead  = false
)

func init() {
	defer func() {
		fmt.Printf("enable rocks db direct read: %v\n", directRead)
	}()
	// change to root, and change the minio config too
	s := os.Getenv("MINIO_ROCKS_DB_DIRECT_READ")
	if s == "on" {
		directRead = true
	}
}

func init() {
	defer func() {
		fmt.Printf("set the rocks db root to %v\n", rocksdbRoot)
	}()
	// change to root, and change the minio config too
	s := os.Getenv("MINIO_ROCKS_DB_PATH")
	if s == "" {
		return
	}
	rocksdbRoot = s
}

func init() {
	defer func() {
		fmt.Printf("set rocksdb block cache to %vm\n", blockCache)
	}()
	s := os.Getenv("MINIO_ROCKSDB_BLOCK_CACHE")
	if s == "" {
		return
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return
	}
	blockCache = i
}

func newRocksDBIndex(dir string) (Index, error) {
	path := filepath.Join(rocksdbRoot, dir, "index")
	// path := filepath.Join("/tmp/leveldb", dir, "index")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	opts := gorocksdb.NewDefaultOptions()
	if directRead {
		opts.SetUseDirectReads(true)
	}

	// plain
	// opts.SetPlainTableFactory(0, 10, 0.75, 16)
	// pt := gorocksdb.NewFixedPrefixTransform(8)
	// opts.SetPrefixExtractor(pt)

	// blocked
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(blockCache << 20))
	// bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(10000)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}
	return &rocksDBIndex{
		db: db,
		wo: gorocksdb.NewDefaultWriteOptions(),
		ro: gorocksdb.NewDefaultReadOptions(),
	}, nil
}

func (db *rocksDBIndex) Get(key string) (fi FileInfo, err error) {
	value, err := db.db.Get(db.ro, []byte(key))
	if err != nil {
		return fi, err
	}
	defer value.Free()

	fi.fileName = key
	data := make([]byte, value.Size())
	copy(data, value.Data())

	if strings.HasSuffix(fi.fileName, "xl.json") {
		fi.data = data
		fi.size = uint64(len(data))
		// fi.modTime = time.Now()
		return
	}
	err = fi.UnmarshalBinary(data)
	return
}

func (db *rocksDBIndex) Set(key string, fi FileInfo) error {
	if strings.HasSuffix(key, "xl.json") {
		return db.db.Put(db.wo, []byte(key), fi.data)
	}
	data := fi.MarshalBinary()
	return db.db.Put(db.wo, []byte(key), data)
}

func (db *rocksDBIndex) Delete(key string) error {
	return nil
}

func (db *rocksDBIndex) List(keyPrefix string) ([]string, error) {
	return nil, nil
}

func (db *rocksDBIndex) ListN(keyPrefix string, count int) ([]string, error) {
	return nil, nil
}

func (db *rocksDBIndex) Close() error {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
	return nil
}
