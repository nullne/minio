package volume

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tecbot/gorocksdb"
)

type rocksDBIndex struct {
	db *gorocksdb.DB
	wo *gorocksdb.WriteOptions
	ro *gorocksdb.ReadOptions
}

type RocksDBOptions struct {
	Root       string
	BackupRoot string
	BackupCron string

	DirectRead   bool
	BloomFilter  bool
	MaxOpenFiles int
	BlockCache   int // MB
}

func (opt *RocksDBOptions) setDefaultIfEmpty() {
	if opt.MaxOpenFiles == 0 {
		opt.MaxOpenFiles = 1000
	}
}

func NewRocksDBIndex(dir string, opt RocksDBOptions) (Index, error) {
	opt.setDefaultIfEmpty()
	path := filepath.Join(opt.Root, dir, "index")
	fmt.Println(path)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// set rocksdb options
	opts := gorocksdb.NewDefaultOptions()
	if opt.DirectRead {
		opts.SetUseDirectReads(true)
	}
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	if opt.BlockCache == 0 {
		bbto.SetNoBlockCache(true)
	} else {
		bbto.SetBlockCache(gorocksdb.NewLRUCache(opt.BlockCache << 20))
	}
	if opt.BloomFilter {
		bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))
	}
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(opt.MaxOpenFiles)

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

	if strings.HasSuffix(fi.fileName, xlJSONFile) {
		fi.data = data
		fi.size = uint32(len(data))
		// fi.modTime = time.Now()
		return
	}
	err = fi.UnmarshalBinary(data)
	return
}

func (db *rocksDBIndex) Set(key string, fi FileInfo) error {
	if strings.HasSuffix(key, xlJSONFile) {
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
