package volume

import (
	"os"
	"path/filepath"

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
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

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
	// if strings.HasSuffix(fi.fileName, "xl.json") {
	// 	fi.data = data
	// 	fi.size = uint64(len(data))
	// 	// fi.modTime = time.Now()
	// 	return
	// }
	err = fi.UnmarshalBinary(value.Data())
	return
}

func (db *rocksDBIndex) Set(key string, fi FileInfo) error {
	// if strings.HasSuffix(key, "xl.json") {
	// 	return l.db.Put(opt, []byte(key), fi.data)
	// }
	data := fi.MarshalBinary()
	opt := gorocksdb.NewDefaultWriteOptions()
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
