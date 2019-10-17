package rocksdb

import (
	"bytes"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/multierr"
)

var (
	ErrRocksDBClosed = errors.New("rocksdb has been closed already")
)

type options struct {
	root string

	directRead   bool
	bloomFilter  bool
	maxOpenFiles int
	blockCache   int // MB
	rateLimiter  int //MB
}

// MINIO_ROCKSDB_DIRECT_READ
// MINIO_ROCKSDB_BLOOM_FILTER
// MINIO_ROCKSDB_MAX_OPEN_FILES
// MINIO_ROCKSDB_BLOCK_CACHE
// MINIO_ROCKSDB_RATE_LIMITER
func parseOptionsFromEnv() (opt options) {
	getenv := func(p string) string {
		s := os.Getenv(p)
		return strings.TrimSpace(s)
	}

	if s := getenv("MINIO_ROCKSDB_DIRECT_READ"); s == "on" {
		opt.directRead = true
	}

	if s := getenv("MINIO_ROCKSDB_BLOOM_FILTER"); s == "on" {
		opt.bloomFilter = true
	}

	if s := getenv("MINIO_ROCKSDB_MAX_OPEN_FILES"); s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			opt.maxOpenFiles = i
		}
	}

	if s := getenv("MINIO_ROCKSDB_BLOCK_CACHE"); s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			opt.blockCache = i
		}
	}

	if s := getenv("MINIO_ROCKSDB_RATE_LIMITER"); s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			opt.rateLimiter = i
		}
	}
	return opt
}

func toRocksdbOptions(opt options) *gorocksdb.Options {
	opt.setDefaultIfEmpty()
	opts := gorocksdb.NewDefaultOptions()
	if opt.directRead {
		opts.SetUseDirectReads(true)
	}
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	if opt.blockCache == 0 {
		bbto.SetNoBlockCache(true)
	} else {
		bbto.SetBlockCache(gorocksdb.NewLRUCache(opt.blockCache << 20))
	}
	if opt.bloomFilter {
		bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))
	}
	if opt.rateLimiter != 0 {
		limiter := gorocksdb.NewRateLimiter(int64(opt.rateLimiter<<20), 100000, 10)
		// defer limiter.Destroy()
		opts.SetRateLimiter(limiter)
	}
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(opt.maxOpenFiles)
	return opts
}

func (opt *options) setDefaultIfEmpty() {
	if opt.maxOpenFiles == 0 {
		opt.maxOpenFiles = 1000
	}
}

type rocksDBIndex struct {
	db *gorocksdb.DB

	opts *gorocksdb.Options
	wo   *gorocksdb.WriteOptions
	ro   *gorocksdb.ReadOptions

	closed uint32
	wg     sync.WaitGroup
}

func NewIndex(name string, options volume.IndexOptions) (volume.Index, error) {
	p := filepath.Join(options.Root, name, volume.IndexDir)
	if err := volume.MkdirIfNotExist(p); err != nil {
		return nil, err
	}

	opts := toRocksdbOptions(parseOptionsFromEnv())
	db, err := gorocksdb.OpenDb(opts, p)
	if err != nil {
		return nil, err
	}
	idx := rocksDBIndex{
		db:   db,
		wo:   gorocksdb.NewDefaultWriteOptions(),
		ro:   gorocksdb.NewDefaultReadOptions(),
		opts: opts,
	}

	return &idx, nil
}

func (db *rocksDBIndex) Get(key string) ([]byte, error) {
	db.wg.Add(1)
	defer db.wg.Done()
	if db.isClosed() {
		return nil, interfaces.ErrDBClosed
	}
	value, err := db.db.GetBytes(db.ro, []byte(key))
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, interfaces.ErrKeyNotExisted
	}
	return value, nil
}

func (db *rocksDBIndex) Set(key string, data []byte) error {
	db.wg.Add(1)
	defer db.wg.Done()
	if db.isClosed() {
		return interfaces.ErrDBClosed
	}
	return db.db.Put(db.wo, []byte(key), data)
}

func (db *rocksDBIndex) Delete(keyPrefix string) (err error) {
	db.wg.Add(1)
	defer db.wg.Done()
	if db.isClosed() {
		return interfaces.ErrDBClosed
	}
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
	if len(keys) == 0 {
		return interfaces.ErrNotExisted
	}
	for _, key := range keys {
		if err := db.db.Delete(db.wo, key); err != nil {
			return err
		}
	}
	return nil
}

func (db *rocksDBIndex) ListN(keyPrefix, leaf string, count int) ([]string, error) {
	if db.isClosed() {
		return nil, interfaces.ErrDBClosed
	}
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	it := db.db.NewIterator(ro)
	defer it.Close()

	var entries []string
	if count > 0 {
		entries = make([]string, 0, count)
	}

	it.Seek([]byte(keyPrefix))
	if !it.Valid() { // not found
		return nil, interfaces.ErrNotExisted
	}

	for count != 0 {
		if db.isClosed() {
			return nil, interfaces.ErrDBClosed
		}
		if !it.ValidForPrefix([]byte(keyPrefix)) {
			break
		}
		key := it.Key()
		skey := string(key.Data())
		entry := volume.SubDir(skey, keyPrefix)
		if entry == "" {
			key.Free()
			it.Next()
			continue
		}

		if leaf != "" {
			if _, err := db.Get(path.Join(keyPrefix, entry, leaf)); err == nil {
				entry = strings.TrimRight(entry, "/")
			} else if err != nil && err != interfaces.ErrKeyNotExisted {
				return nil, err
			}
		}

		entries = append(entries, entry)
		count--

		key.Free()

		for {
			if db.isClosed() {
				return nil, interfaces.ErrDBClosed
			}
			it.Next()
			if !it.ValidForPrefix([]byte(volume.PathJoin(keyPrefix, entry))) {
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
	if db.isClosed() {
		return nil
	}
	atomic.StoreUint32(&(db.closed), 1)
	db.wg.Wait()
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
	db.opts.Destroy()
	return nil
}

func (db *rocksDBIndex) Remove() (err error) {
	name := db.db.Name()
	err = multierr.Append(err, db.Close())

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	err = multierr.Append(err, gorocksdb.DestroyDb(name, opts))

	err = multierr.Append(err, os.RemoveAll(path.Dir(strings.TrimRight(name, "/"))))

	return err
}

func (db *rocksDBIndex) isClosed() bool {
	return atomic.LoadUint32(&(db.closed)) == 1
}
