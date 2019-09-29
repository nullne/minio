// list avalaible options here
package rocksdb

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio/pkg/volume"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/multierr"
)

const (
	xlJSONFile = ".json"
)

type rocksDBIndex struct {
	db        *gorocksdb.DB
	backupDir string

	opts *gorocksdb.Options
	wo   *gorocksdb.WriteOptions
	ro   *gorocksdb.ReadOptions

	closed uint32

	wg *sync.WaitGroup
}

const (
	backupStartTimeLayout string = "15:04:05"
)

var (
	ErrRocksDBClosed = errors.New("rocksdb has been closed already")
)

type options struct {
	root               string
	backupRoot         string
	backupInterval     string
	backupStartBetween string // 02:00:00-04:00:00

	directRead   bool
	bloomFilter  bool
	maxOpenFiles int
	blockCache   int // MB
	rateLimiter  int //MB
}

func parseOptionsFromEnv() (opt options) {
	getenv := func(p string) string {
		s := os.Getenv(p)
		return strings.TrimSpace(s)
	}

	if s := getenv("ROCKSDB_ROOT"); s != "" {
		opt.root = s
	}

	if s := getenv("ROCKSDB_BACKUP_ROOT"); s != "" {
		opt.backupRoot = s
	}

	if s := getenv("ROCKSDB_BACKUP_START_BETWEEN"); s != "" {
		opt.backupStartBetween = s
	}

	if s := getenv("ROCKSDB_BACKUP_INTERVAL"); s != "" {
		opt.backupInterval = s
	}

	if s := getenv("ROCKSDB_DIRECT_READ"); s == "on" {
		opt.directRead = true
	}

	if s := getenv("ROCKSDB_BLOOM_FILTER"); s == "on" {
		opt.bloomFilter = true
	}

	if s := getenv("ROCKSDB_MAX_OPEN_FILES"); s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			opt.maxOpenFiles = i
		}
	}

	if s := getenv("ROCKSDB_BLOCK_CACHE"); s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			opt.blockCache = i
		}
	}

	if s := getenv("ROCKSDB_RATE_LIMITER"); s != "" {
		i, err := strconv.Atoi(s)
		if err == nil {
			opt.rateLimiter = i
		}
	}
	return opt
}

func (opt *options) setDefaultIfEmpty() {
	if opt.maxOpenFiles == 0 {
		opt.maxOpenFiles = 1000
	}
}

func RestoreFromBackup(backupPath, p string) error {
	opt := parseOptionsFromEnv()
	engine, err := gorocksdb.OpenBackupEngine(toRocksdbOptions(opt), path.Join(backupPath, volume.IndexBackupDir))
	defer engine.Close()
	if err != nil {
		return err
	}
	p = path.Join(p, volume.IndexDir)
	ro := gorocksdb.NewRestoreOptions()
	return engine.RestoreDBFromLatestBackup(p, p, ro)
}

func parseObjectKey(key string) string {
	if strings.HasSuffix(key, xlJSONFile) {
		return strings.TrimSuffix(key, "/"+xlJSONFile)
	}
	if strings.HasSuffix(key, "/") {
		return strings.TrimSuffix(key, "/")
	}
	return ""
}

func DumpObjects(p string) (chan string, chan error, error) {
	p = path.Join(p, volume.IndexDir)
	opt := parseOptionsFromEnv()
	db, err := gorocksdb.OpenDbForReadOnly(toRocksdbOptions(opt), p, false)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan string)
	ech := make(chan error, 1)
	go func() {
		defer db.Close()
		defer close(ch)
		defer close(ech)
		ro := gorocksdb.NewDefaultReadOptions()
		ro.SetFillCache(false)
		defer ro.Destroy()
		it := db.NewIterator(ro)
		it.SeekToFirst()
		defer it.Close()
		for it = it; it.Valid(); it.Next() {
			key := it.Key()
			if k := parseObjectKey(string(key.Data())); k != "" {
				ch <- k
			}
			key.Free()
		}
		if err := it.Err(); err != nil {
			ech <- err
		}
	}()
	return ch, ech, nil
}

func Check(p string) error {
	p = path.Join(p, volume.IndexDir)
	opt := parseOptionsFromEnv()
	db, err := gorocksdb.OpenDb(toRocksdbOptions(opt), p)
	if err != nil {
		return err
	}
	db.Close()
	return nil
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

func NewIndex(name string, options volume.IndexOptions) (volume.Index, error) {
	p := filepath.Join(options.Root, name, volume.IndexDir)
	if err := volume.MkdirIfNotExist(p); err != nil {
		return nil, err
	}

	opt := parseOptionsFromEnv()
	opts := toRocksdbOptions(opt)
	db, err := gorocksdb.OpenDb(opts, p)
	if err != nil {
		return nil, err
	}
	idx := rocksDBIndex{
		db:   db,
		wo:   gorocksdb.NewDefaultWriteOptions(),
		ro:   gorocksdb.NewDefaultReadOptions(),
		wg:   &sync.WaitGroup{},
		opts: opts,
	}

	// // backup every interval
	// if opt.BackupRoot != "" && opt.BackupInterval != "" {
	// 	duration, err := time.ParseDuration(opt.BackupInterval)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	//
	// 	// start after time A which pick from time range randomly
	// 	var startAfter time.Duration
	// 	if ss := strings.Split(opt.BackupStartBetween, "-"); len(ss) == 2 {
	// 		startAfter, err = randomPickFromTimeRange(ss[0], ss[1])
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 	}
	// 	volume.backupDir = pathJoin(opt.BackupRoot, dir, volume.IndexBackupDir)
	// 	go volume.backupEvery(volume.backupDir, duration, startAfter)
	// }

	return &idx, nil
}

func randomPickFromTimeRange(sStart, sEnd string) (time.Duration, error) {
	start, err := time.Parse(backupStartTimeLayout, sStart)
	if err != nil {
		return 0, err
	}

	end, err := time.Parse(backupStartTimeLayout, sEnd)
	if err != nil {
		return 0, err
	}
	if start.After(end) {
		return 0, errors.New("invalid backup start time range")
	}
	rand.Seed(int64(time.Now().Nanosecond()))
	start = start.Add(time.Duration(rand.Int63n(end.Sub(start).Nanoseconds())))
	now := time.Now()
	y, m, d := now.Date()
	start = time.Date(y, m, d, start.Hour(), start.Minute(), start.Second(), start.Nanosecond(), now.Location())
	if start.Before(now) {
		start = start.Add(24 * time.Hour)
	}
	return start.Sub(now), nil
}

// keep the trailing slash which indicates a directory
// func (db rocksDBIndex) segment(key string) []string {
// 	endWithSlash := strings.HasSuffix(key, "/")
// 	key = strings.Trim(key, "/")
// 	if key == "" {
// 		return nil
// 	}
// 	ss := strings.Split(key, "/")
// 	segments := make([]string, db.cacheLevel)
// 	for i := 0; i < db.cacheLevel && i < len(ss); i++ {
// 		if i != len(ss)-1 || endWithSlash {
// 			segments[i] = ss[i] + "/"
// 		}
// 	}
// 	return segments
// }

// func (db *rocksDBIndex) initDirectoryCache() {
// 	init := func() error {
// 		defer catchPanic()
// 		defer db.wg.Done()
// 		if db.closed {
// 			return nil
// 		}
// 		ro := gorocksdb.NewDefaultReadOptions()
// 		ro.SetFillCache(false)
// 		defer ro.Destroy()
// 		it := db.db.NewIterator(ro)
// 		it.SeekToFirst()
// 		defer it.Close()
// 		for it = it; it.Valid(); it.Next() {
// 			key := it.Key()
// 			db.directory.put(db.segment(string(key.Data())))
// 			key.Free()
// 		}
// 		if err := it.Err(); err != nil {
// 			db.directoryStatus.Store(directoryStatusUnknown)
// 			return err
// 		}
// 		db.directoryStatus.Store(directoryStatusWorking)
// 		return nil
// 	}
//
// 	initJob()
// 	result := make(chan error, 1)
// 	globalJobQueue <- job{
// 		name:   fmt.Sprintf("init directory cache for rocksdb %s", db.db.Name()),
// 		fn:     init,
// 		result: result,
// 		before: func() { db.wg.Add(1) },
// 		expire: time.Now().Add(time.Hour * 24 * 365), // never expire
// 	}
// 	// may panic in rare case
// 	<-result
// }

func (db *rocksDBIndex) Get(key string) (fi volume.FileInfo, err error) {
	if db.isClosed() {
		return fi, volume.ErrDBClosed
	}
	// defer func(before time.Time) {
	// 	DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-Get"}).Observe(time.Since(before).Seconds())
	// }(time.Now())
	// key = pathJoin(key)
	value, err := db.db.GetBytes(db.ro, []byte(key))
	if err != nil {
		return fi, err
	}
	if value == nil {
		return fi, volume.ErrKeyNotExisted
	}

	fi = volume.NewFileInfo(key)

	if volume.DirectIndexSaving(fi.Name()) {
		fi.Set(value, uint32(len(value)))
		// fi.data = value
		// fi.size = uint32(len(value))
		// fi.modTime = time.Now() // mod time is omitted
		return
	}
	err = fi.UnmarshalBinary(value)
	return
}

func (db *rocksDBIndex) Set(key string, fi volume.FileInfo) (err error) {
	if db.isClosed() {
		return volume.ErrDBClosed
	}
	// defer func(before time.Time) {
	// 	DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-Set"}).Observe(time.Since(before).Seconds())
	// }(time.Now())

	// directory cache
	// defer func() {
	// 	if err != nil || db.directoryStatus.Load().(string) == directoryStatusUnknown || db.directory == nil {
	// 		return
	// 	}
	// 	db.directory.put(db.segment(key))
	// }()

	// key = pathJoin(key)
	if volume.DirectIndexSaving(key) {
		return db.db.Put(db.wo, []byte(key), fi.Data())
	}
	return db.db.Put(db.wo, []byte(key), fi.MarshalBinary())
}

func (db *rocksDBIndex) Delete(keyPrefix string) (err error) {
	if db.isClosed() {
		return volume.ErrDBClosed
	}
	// defer func(before time.Time) {
	// 	DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-Delete"}).Observe(time.Since(before).Seconds())
	// }(time.Now())

	// keyPrefix = pathJoin(keyPrefix)
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
		// delete from directory cache
		// @TODO may be inconsistant if the directory has not been inited
		// if db.directoryStatus.Load().(string) == directoryStatusWorking && db.directory != nil {
		// 	db.directory.delete(db.segment(string(key)))
		// }
	}
	return nil
}

// StatPath
func (db *rocksDBIndex) StatDir(key string) (fi volume.FileInfo, err error) {
	if db.isClosed() {
		return fi, volume.ErrDBClosed
	}
	// defer func(before time.Time) {
	// 	DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-StatDir"}).Observe(time.Since(before).Seconds())
	// }(time.Now())
	// key = pathJoin(key)
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
		return volume.NewDirInfo(key), nil
	}

	v := it.Value()
	defer v.Free()
	fi = volume.NewFileInfo(key)
	// fi.fileName = key
	err = fi.UnmarshalBinary(v.Data())
	return
}

// count -1 means unlimited
// listN list count entries under directory keyPrefix, not including itself
func (db *rocksDBIndex) ListN(keyPrefix string, count int) ([]string, error) {
	if db.isClosed() {
		return nil, volume.ErrDBClosed
	}
	// defer func(before time.Time) {
	// 	DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-ListN"}).Observe(time.Since(before).Seconds())
	// }(time.Now())

	// keyPrefix = pathJoin(keyPrefix)

	// list from directory cache
	// if status := db.directoryStatus.Load().(string); status == directoryStatusWorking {
	// 	if seg := db.segment(keyPrefix); len(seg) < db.cacheLevel && db.directory != nil {
	// 		return db.directory.list(seg, count), nil
	// 	}
	// }

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	it := db.db.NewIterator(ro)
	defer it.Close()

	var entryMap map[string]struct{}
	if count <= 0 {
		entryMap = make(map[string]struct{})
	} else {
		entryMap = make(map[string]struct{}, count)
	}

	addToEntries := func(entry string) {
		if _, ok := entryMap[entry]; ok {
			return
		}
		entryMap[entry] = struct{}{}
		count--
	}

	it.Seek([]byte(keyPrefix))
	// the directory not found
	if !it.Valid() {
		return nil, os.ErrNotExist
	}

	for count != 0 {
		if db.isClosed() {
			return nil, ErrRocksDBClosed
		}
		if !it.ValidForPrefix([]byte(keyPrefix)) {
			break
		}
		key := it.Key()
		entry := volume.SubDir(string(key.Data()), keyPrefix)
		if entry == "" {
			key.Free()
			it.Next()
			continue
		}
		addToEntries(entry)

		key.Free()

		for {
			if db.isClosed() {
				return nil, ErrRocksDBClosed
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
	entries := make([]string, 0, len(entryMap))
	for k, _ := range entryMap {
		entries = append(entries, k)
	}
	return entries, nil
}

func (db *rocksDBIndex) ScanAll(ctx context.Context, filter func(s string) bool) (chan volume.FileInfo, chan error) {
	ch := make(chan volume.FileInfo, 1000)
	ech := make(chan error, 1)
	go func(ctx context.Context) {
		defer close(ch)
		defer close(ech)

		if db.isClosed() {
			return
		}
		ro := gorocksdb.NewDefaultReadOptions()
		ro.SetFillCache(false)
		defer ro.Destroy()
		it := db.db.NewIterator(ro)
		defer it.Close()

		it.SeekToFirst()
		breaks := false

		for it = it; it.Valid() && !breaks; it.Next() {
			key := it.Key()
			if filter(string(key.Data())) {
				// var fi volume.FileInfo
				// fi.fileName = string(key.Data())
				fi := volume.NewFileInfo(string(key.Data()))
				value := it.Value()
				err := fi.UnmarshalBinary(value.Data())
				if err != nil {
					select {
					case ech <- err:
					case <-ctx.Done():
						breaks = true
					}
				} else {
					select {
					case ch <- fi:
					case <-ctx.Done():
						breaks = true
					}
				}
				value.Free()
			}
			key.Free()
		}
		if err := it.Err(); err != nil {
			ech <- err
			return
		}
	}(ctx)
	return ch, ech
}

func (db *rocksDBIndex) Close() error {
	if db.isClosed() {
		return nil
	}
	db.wg.Wait()
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
	db.opts.Destroy()
	atomic.StoreUint32(&(db.closed), 1)
	return nil
}

func (db *rocksDBIndex) Remove() (err error) {
	// defer func(before time.Time) {
	// 	DiskOperationDuration.With(prometheus.Labels{"operation_type": "Remove"}).Observe(time.Since(before).Seconds())
	// }(time.Now())
	name := db.db.Name()
	err = multierr.Append(err, db.Close())

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	err = multierr.Append(err, gorocksdb.DestroyDb(name, opts))

	err = multierr.Append(err, os.RemoveAll(path.Dir(strings.TrimRight(name, "/"))))

	if db.backupDir != "" {
		err = multierr.Append(err, os.RemoveAll(path.Dir(strings.TrimRight(db.backupDir, "/"))))
	}
	return err
}

func (db *rocksDBIndex) backupFn(p string) func() error {
	return func() error {
		defer db.wg.Done()
		if db.isClosed() {
			return nil
		}
		engine, err := gorocksdb.OpenBackupEngine(db.opts, p)
		if err != nil {
			return err
		}
		defer engine.Close()
		return engine.CreateNewBackup(db.db)
	}
}

func (db *rocksDBIndex) backupEvery(p string, interval, startAfter time.Duration) {
	// initJob()
	// time.Sleep(startAfter)
	// for _ = range time.Tick(interval) {
	// 	if db.isClosed() {
	// 		return
	// 	}
	// 	result := make(chan error, 1)
	// 	globalJobQueue <- job{
	// 		name:   fmt.Sprintf("backup rocksdb %s to %s", db.db.Name(), p),
	// 		before: func() { db.wg.Add(1) },
	// 		fn:     db.backupFn(p),
	// 		result: result,
	// 		expire: time.Now().Add(interval),
	// 	}
	// }
}

func (db *rocksDBIndex) isClosed() bool {
	return atomic.LoadUint32(&(db.closed)) == 1
}
