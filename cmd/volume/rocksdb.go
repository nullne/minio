package volume

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/tecbot/gorocksdb"
)

type rocksDBIndex struct {
	db *gorocksdb.DB
}

func newRocksDBIndex(dir string) (Index, error) {
	path := filepath.Join("/var/minio/rocksdb", dir, "index")
	// path := filepath.Join("/tmp/leveldb", dir, "index")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(2 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(10000)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}
	return &rocksDBIndex{db: db}, nil
}

func (db *rocksDBIndex) Get(key string) (fi FileInfo, err error) {
	opt := gorocksdb.NewDefaultReadOptions()
	value, err := db.db.Get(opt, []byte(key))
	if err != nil {
		return fi, err
	}
	defer value.Free()

	fi.fileName = key
	data := value.Data()

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
	opt := gorocksdb.NewDefaultWriteOptions()
	if strings.HasSuffix(key, "xl.json") {
		return db.db.Put(opt, []byte(key), fi.data)
	}
	data := fi.MarshalBinary()
	return db.db.Put(opt, []byte(key), data)
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
	return nil
}
