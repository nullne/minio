package volume

import (
	"bytes"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
	defer opts.Destroy()
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
	key = pathJoin(key)
	value, err := db.db.GetBytes(db.ro, []byte(key))
	if err != nil {
		return fi, err
	}
	if value == nil {
		return fi, os.ErrNotExist
	}

	fi.fileName = key

	if strings.HasSuffix(fi.fileName, xlJSONFile) {
		fi.data = value
		fi.size = uint32(len(value))
		// fi.modTime = time.Now()
		return
	}
	err = fi.UnmarshalBinary(value)
	return
}

func (db *rocksDBIndex) Set(key string, fi FileInfo) error {
	key = pathJoin(key)
	if strings.HasSuffix(key, xlJSONFile) {
		return db.db.Put(db.wo, []byte(key), fi.data)
	}
	data := fi.MarshalBinary()
	return db.db.Put(db.wo, []byte(key), data)
}

func (db *rocksDBIndex) Delete(keyPrefix string) error {
	keyPrefix = pathJoin(keyPrefix)
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	it := db.db.NewIterator(ro)
	defer it.Close()

	var keys [][]byte

	it.Seek([]byte(keyPrefix))
	for it = it; it.ValidForPrefix([]byte(keyPrefix)); it.Next() {
		key := it.Key()
		defer key.Free()
		remaining := bytes.TrimPrefix(key.Data(), []byte(keyPrefix))
		remaining = bytes.TrimPrefix(remaining, []byte{'/'})
		if idx := bytes.Index(remaining, []byte{'/'}); idx != -1 {
			// dont' delete not empty directory
			return syscall.ENOTEMPTY
		}
		// must copy or something strange happens
		b := make([]byte, key.Size())
		copy(b, key.Data())
		keys = append(keys, b)
	}
	if err := it.Err(); err != nil {
		return err
	}
	for _, key := range keys {
		if err := db.db.Delete(db.wo, key); err != nil {
			return err
		}
	}
	return nil
}

// StatPath
func (db *rocksDBIndex) StatDir(key string) (fi FileInfo, err error) {
	key = pathJoin(key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	it := db.db.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte(key))
	if !it.Valid() {
		return fi, os.ErrNotExist
	}
	k := it.Key()
	defer k.Free()
	if !bytes.Equal(k.Data(), []byte(key)) {
		return FileInfo{
			fileName: key,
			isDir:    true,
			// faked time
			modTime: time.Now(),
		}, nil
	}

	v := it.Value()
	defer v.Free()
	fi.fileName = key
	err = fi.UnmarshalBinary(v.Data())
	return
}

// count -1 means unlimited
// listN list count entries under directory keyPrefix, not including itself
func (db *rocksDBIndex) ListN(keyPrefix string, count int) ([]string, error) {
	keyPrefix = pathJoin(keyPrefix)
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

func (db *rocksDBIndex) Remove() error {
	name := db.db.Name()
	if err := db.Close(); err != nil {
		return err
	}
	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	return gorocksdb.DestroyDb(name, opts)
}

// p1		p2	 	result
// a/b/c 	        a/
// a               a
// a       a
// a/b/c  	a       b/
// aa/b/c 	a
// a/b/c 	b
// p1, p2 will never start with '/'
func subDir(p1, p2 string) string {
	if p2 == "" {
		goto firstPart
	}
	if !strings.HasSuffix(p2, "/") {
		p2 += "/"
	}
	if !strings.HasPrefix(p1, p2) {
		return ""
	}
	p1 = strings.TrimPrefix(p1, p2)
firstPart:
	p1 = strings.TrimPrefix(p1, "/")
	idx := strings.Index(p1, "/")
	if idx == -1 {
		return p1
	}
	return p1[:idx+1]
}
