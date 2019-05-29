package volume

import (
	"os"
	"path"
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
	return db.listN(keyPrefix, -1)
}

func (db *rocksDBIndex) ListN(keyPrefix string, count int) ([]string, error) {
	return db.listN(keyPrefix, count)
}

func (db *rocksDBIndex) listN(keyPrefix string, count int) ([]string, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	it := db.db.NewIterator(ro)
	defer it.Close()

	var entries []string

	addToEntries := func(entry string) {
		found := false
		for _, e := range entries {
			if e == entry {
				found = true
				break
			}
		}
		if found {
			return
		}
		entries = append(entries, entry)
		count--
	}

	it.Seek([]byte(keyPrefix))
	for count != 0 {
		if !it.Valid() {
			break
		}
		key := it.Key()
		entry := subDir(string(key.Data()), keyPrefix)
		if entry == "" {
			it.Next()
			continue
		}
		addToEntries(entry)

		key.Free()

		// it.SeekForPrev([]byte(path.Join(keyPrefix + entry)))
		for {
			it.Next()
			if !it.ValidForPrefix([]byte(path.Join(keyPrefix + entry))) {
				break
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (db *rocksDBIndex) Close() error {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
	return nil
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
