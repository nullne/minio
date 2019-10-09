package volume

//
// import (
// 	"bytes"
// 	"context"
// 	"errors"
// 	"fmt"
// 	"math/rand"
// 	"os"
// 	"path"
// 	"path/filepath"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"syscall"
// 	"time"
//
// 	"github.com/minio/minio/cmd/logger"
// 	"github.com/prometheus/client_golang/prometheus"
// 	"github.com/tecbot/gorocksdb"
// 	"go.uber.org/multierr"
// )
//
// type rocksDBIndex struct {
// 	db *gorocksdb.DB
// 	wo *gorocksdb.WriteOptions
// 	ro *gorocksdb.ReadOptions
//
// 	wg *sync.WaitGroup
//
// 	opts *gorocksdb.Options
//
// 	// cache the first N level directories
// 	directory       *pathTrie
// 	directoryStatus atomic.Value //string
// 	cacheLevel      int
//
// 	backupDir string
//
// 	closed bool
// }
//
// const (
// 	directoryStatusUnknown string = "unknown"
// 	directoryStatusIniting string = "initing"
// 	directoryStatusWorking string = "working"
// )
//
// const (
// 	backupStartTimeLayout string = "15:04:05"
// )
//
// var (
// 	ErrRocksDBClosed = errors.New("rocksdb has been closed already")
// )
//
// type RocksDBOptions struct {
// 	Root               string
// 	BackupRoot         string
// 	BackupInterval     string
// 	BackupStartBetween string // 02:00:00-04:00:00
//
// 	DirectRead   bool
// 	BloomFilter  bool
// 	MaxOpenFiles int
// 	BlockCache   int // MB
// 	RateLimiter  int //MB
//
// 	CacheLevel int
// }
//
// func parseRocksDBOptionsFromEnv() RocksDBOptions {
// 	opt := RocksDBOptions{}
// 	getenv := func(p string) string {
// 		s := os.Getenv(p)
// 		return strings.TrimSpace(s)
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_ROOT"); s != "" {
// 		opt.Root = s
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_BACKUP_ROOT"); s != "" {
// 		opt.BackupRoot = s
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_BACKUP_START_BETWEEN"); s != "" {
// 		opt.BackupStartBetween = s
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_BACKUP_INTERVAL"); s != "" {
// 		opt.BackupInterval = s
// 	}
//
// 	if s := getenv("MINIO_DIRECT_READ"); s == "on" {
// 		opt.DirectRead = true
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_BLOOM_FILTER"); s == "on" {
// 		opt.BloomFilter = true
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_MAX_OPEN_FILES"); s != "" {
// 		i, err := strconv.Atoi(s)
// 		if err == nil {
// 			opt.MaxOpenFiles = i
// 		}
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_BLOCK_CACHE"); s != "" {
// 		i, err := strconv.Atoi(s)
// 		if err == nil {
// 			opt.BlockCache = i
// 		}
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_RATE_LIMITER"); s != "" {
// 		i, err := strconv.Atoi(s)
// 		if err == nil {
// 			opt.RateLimiter = i
// 		}
// 	}
//
// 	if s := getenv("MINIO_ROCKSDB_DIRECTORY_CACHE_LEVEL"); s != "" {
// 		i, err := strconv.Atoi(s)
// 		if err == nil {
// 			opt.CacheLevel = i
// 		}
// 	}
// 	return opt
// }
//
// func (opt *RocksDBOptions) setDefaultIfEmpty() {
// 	if opt.MaxOpenFiles == 0 {
// 		opt.MaxOpenFiles = 1000
// 	}
// }
//
// func RestoreRocksDBFromBackup(backupPath, p string) error {
// 	opt := parseRocksDBOptionsFromEnv()
// 	engine, err := gorocksdb.OpenBackupEngine(rocksdbOptions(opt), path.Join(backupPath, IndexBackupDir))
// 	defer engine.Close()
// 	if err != nil {
// 		return err
// 	}
// 	p = path.Join(p, IndexDir)
// 	ro := gorocksdb.NewRestoreOptions()
// 	return engine.RestoreDBFromLatestBackup(p, p, ro)
// }
//
// func parseObjectKey(key string) string {
// 	if strings.HasSuffix(key, xlJSONFile) {
// 		return strings.TrimSuffix(key, "/"+xlJSONFile)
// 	}
// 	if strings.HasSuffix(key, "/") {
// 		return strings.TrimSuffix(key, "/")
// 	}
// 	return ""
// }
//
// func DumpObjectsFromRocksDB(p string) (chan string, error) {
// 	p = path.Join(p, IndexDir)
// 	opt := parseRocksDBOptionsFromEnv()
// 	db, err := gorocksdb.OpenDbForReadOnly(rocksdbOptions(opt), p, false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	ch := make(chan string)
// 	go func() {
// 		defer db.Close()
// 		defer close(ch)
// 		ro := gorocksdb.NewDefaultReadOptions()
// 		ro.SetFillCache(false)
// 		defer ro.Destroy()
// 		it := db.NewIterator(ro)
// 		it.SeekToFirst()
// 		defer it.Close()
// 		for it = it; it.Valid(); it.Next() {
// 			key := it.Key()
// 			if k := parseObjectKey(string(key.Data())); k != "" {
// 				ch <- k
// 			}
// 			key.Free()
// 		}
// 		if err := it.Err(); err != nil {
// 			logger.Fatal(err, "failed to dump objects")
// 		}
// 	}()
// 	return ch, nil
// }
//
// func CheckRocksDB(p string) error {
// 	p = path.Join(p, IndexDir)
// 	opt := parseRocksDBOptionsFromEnv()
// 	db, err := gorocksdb.OpenDb(rocksdbOptions(opt), p)
// 	if err != nil {
// 		return err
// 	}
// 	db.Close()
// 	return nil
// }
//
// // set rocksdb options
// func rocksdbOptions(opt RocksDBOptions) *gorocksdb.Options {
// 	opt.setDefaultIfEmpty()
// 	opts := gorocksdb.NewDefaultOptions()
// 	if opt.DirectRead {
// 		opts.SetUseDirectReads(true)
// 	}
// 	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
// 	if opt.BlockCache == 0 {
// 		bbto.SetNoBlockCache(true)
// 	} else {
// 		bbto.SetBlockCache(gorocksdb.NewLRUCache(opt.BlockCache << 20))
// 	}
// 	if opt.BloomFilter {
// 		bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))
// 	}
// 	if opt.RateLimiter != 0 {
// 		limiter := gorocksdb.NewRateLimiter(int64(opt.RateLimiter<<20), 100000, 10)
// 		// defer limiter.Destroy()
// 		opts.SetRateLimiter(limiter)
// 	}
// 	opts.SetBlockBasedTableFactory(bbto)
//
// 	opts.SetCreateIfMissing(true)
// 	opts.SetMaxOpenFiles(opt.MaxOpenFiles)
// 	return opts
// }
//
// func NewRocksDBIndex(dir string, opt RocksDBOptions) (Index, error) {
// 	path := filepath.Join(opt.Root, dir, IndexDir)
// 	if _, err := os.Stat(path); os.IsNotExist(err) {
// 		if err := os.MkdirAll(path, 0755); err != nil {
// 			return nil, err
// 		}
// 	} else if err != nil {
// 		return nil, err
// 	}
//
// 	opts := rocksdbOptions(opt)
// 	db, err := gorocksdb.OpenDb(opts, path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	index := rocksDBIndex{
// 		db:   db,
// 		wo:   gorocksdb.NewDefaultWriteOptions(),
// 		ro:   gorocksdb.NewDefaultReadOptions(),
// 		wg:   &sync.WaitGroup{},
// 		opts: opts,
// 	}
// 	index.directoryStatus.Store(directoryStatusUnknown)
// 	if opt.CacheLevel > 0 {
// 		index.directoryStatus.Store(directoryStatusIniting)
// 		index.directory = newPathTrie()
// 		index.cacheLevel = opt.CacheLevel
// 		go index.initDirectoryCache()
// 	}
//
// 	// backup every interval
// 	if opt.BackupRoot != "" && opt.BackupInterval != "" {
// 		duration, err := time.ParseDuration(opt.BackupInterval)
// 		if err != nil {
// 			return nil, err
// 		}
//
// 		// start after time A which pick from time range randomly
// 		var startAfter time.Duration
// 		if ss := strings.Split(opt.BackupStartBetween, "-"); len(ss) == 2 {
// 			startAfter, err = randomPickFromTimeRange(ss[0], ss[1])
// 			if err != nil {
// 				return nil, err
// 			}
// 		}
// 		index.backupDir = pathJoin(opt.BackupRoot, dir, IndexBackupDir)
// 		go index.backupEvery(index.backupDir, duration, startAfter)
// 	}
//
// 	return &index, nil
// }
//
// func randomPickFromTimeRange(sStart, sEnd string) (time.Duration, error) {
// 	start, err := time.Parse(backupStartTimeLayout, sStart)
// 	if err != nil {
// 		return 0, err
// 	}
//
// 	end, err := time.Parse(backupStartTimeLayout, sEnd)
// 	if err != nil {
// 		return 0, err
// 	}
// 	if start.After(end) {
// 		return 0, errors.New("invalid backup start time range")
// 	}
// 	rand.Seed(int64(time.Now().Nanosecond()))
// 	start = start.Add(time.Duration(rand.Int63n(end.Sub(start).Nanoseconds())))
// 	now := time.Now()
// 	y, m, d := now.Date()
// 	start = time.Date(y, m, d, start.Hour(), start.Minute(), start.Second(), start.Nanosecond(), now.Location())
// 	if start.Before(now) {
// 		start = start.Add(24 * time.Hour)
// 	}
// 	return start.Sub(now), nil
// }
//
// // keep the trailing slash which indicates a directory
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
//
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
//
// func (db *rocksDBIndex) backupFn(p string) func() error {
// 	return func() error {
// 		defer catchPanic()
// 		defer db.wg.Done()
// 		if db.closed {
// 			return nil
// 		}
// 		engine, err := gorocksdb.OpenBackupEngine(db.opts, p)
// 		if err != nil {
// 			return err
// 		}
// 		defer engine.Close()
// 		return engine.CreateNewBackup(db.db)
// 	}
// }
//
// func (db *rocksDBIndex) backupEvery(p string, interval, startAfter time.Duration) {
// 	initJob()
// 	logger.Info("sleep %s before backup", startAfter.String())
// 	time.Sleep(startAfter)
// 	for _ = range time.Tick(interval) {
// 		if db.closed {
// 			return
// 		}
// 		result := make(chan error, 1)
// 		globalJobQueue <- job{
// 			name:   fmt.Sprintf("backup rocksdb %s to %s", db.db.Name(), p),
// 			before: func() { db.wg.Add(1) },
// 			fn:     db.backupFn(p),
// 			result: result,
// 			expire: time.Now().Add(interval),
// 		}
// 	}
// }
//
// func (db *rocksDBIndex) Get(key string) (fi FileInfo, err error) {
// 	if db.closed {
// 		return fi, ErrRocksDBClosed
// 	}
// 	defer func(before time.Time) {
// 		DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-Get"}).Observe(time.Since(before).Seconds())
// 	}(time.Now())
// 	// key = pathJoin(key)
// 	value, err := db.db.GetBytes(db.ro, []byte(key))
// 	if err != nil {
// 		return fi, err
// 	}
// 	if value == nil {
// 		return fi, os.ErrNotExist
// 	}
//
// 	fi.fileName = key
//
// 	if strings.HasSuffix(fi.fileName, xlJSONFile) {
// 		fi.data = value
// 		fi.size = uint32(len(value))
// 		// fi.modTime = time.Now()
// 		return
// 	}
// 	err = fi.UnmarshalBinary(value)
// 	return
// }
//
// func (db *rocksDBIndex) Set(key string, fi FileInfo) (err error) {
// 	if db.closed {
// 		return ErrRocksDBClosed
// 	}
// 	defer func(before time.Time) {
// 		DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-Set"}).Observe(time.Since(before).Seconds())
// 	}(time.Now())
//
// 	// directory cache
// 	defer func() {
// 		if err != nil || db.directoryStatus.Load().(string) == directoryStatusUnknown || db.directory == nil {
// 			return
// 		}
// 		db.directory.put(db.segment(key))
// 	}()
//
// 	// key = pathJoin(key)
// 	if strings.HasSuffix(key, xlJSONFile) {
// 		return db.db.Put(db.wo, []byte(key), fi.data)
// 	}
// 	data := fi.MarshalBinary()
// 	return db.db.Put(db.wo, []byte(key), data)
// }
//
// func (db *rocksDBIndex) Delete(keyPrefix string) (err error) {
// 	if db.closed {
// 		return ErrRocksDBClosed
// 	}
// 	defer func(before time.Time) {
// 		DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-Delete"}).Observe(time.Since(before).Seconds())
// 	}(time.Now())
//
// 	// keyPrefix = pathJoin(keyPrefix)
// 	ro := gorocksdb.NewDefaultReadOptions()
// 	ro.SetFillCache(false)
// 	defer ro.Destroy()
// 	it := db.db.NewIterator(ro)
// 	defer it.Close()
//
// 	var keys [][]byte
//
// 	it.Seek([]byte(keyPrefix))
// 	for it = it; it.ValidForPrefix([]byte(keyPrefix)); it.Next() {
// 		key := it.Key()
// 		defer key.Free()
// 		remaining := bytes.TrimPrefix(key.Data(), []byte(keyPrefix))
// 		remaining = bytes.TrimPrefix(remaining, []byte{'/'})
// 		if idx := bytes.Index(remaining, []byte{'/'}); idx != -1 {
// 			// dont' delete not empty directory
// 			return syscall.ENOTEMPTY
// 		}
// 		// must copy or something strange happens
// 		b := make([]byte, key.Size())
// 		copy(b, key.Data())
// 		keys = append(keys, b)
// 	}
// 	if err := it.Err(); err != nil {
// 		return err
// 	}
// 	for _, key := range keys {
// 		if err := db.db.Delete(db.wo, key); err != nil {
// 			return err
// 		}
// 		// delete from directory cache
// 		// @TODO may be inconsistant if the directory has not been inited
// 		if db.directoryStatus.Load().(string) == directoryStatusWorking && db.directory != nil {
// 			db.directory.delete(db.segment(string(key)))
// 		}
// 	}
// 	return nil
// }
//
// // StatPath
// func (db *rocksDBIndex) StatDir(key string) (fi FileInfo, err error) {
// 	if db.closed {
// 		return fi, ErrRocksDBClosed
// 	}
// 	defer func(before time.Time) {
// 		DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-StatDir"}).Observe(time.Since(before).Seconds())
// 	}(time.Now())
// 	// key = pathJoin(key)
// 	if !strings.HasSuffix(key, "/") {
// 		key += "/"
// 	}
// 	ro := gorocksdb.NewDefaultReadOptions()
// 	ro.SetFillCache(false)
// 	defer ro.Destroy()
// 	it := db.db.NewIterator(ro)
// 	defer it.Close()
// 	it.Seek([]byte(key))
// 	if !it.Valid() {
// 		return fi, os.ErrNotExist
// 	}
// 	k := it.Key()
// 	defer k.Free()
// 	if !bytes.Equal(k.Data(), []byte(key)) {
// 		return FileInfo{
// 			fileName: key,
// 			isDir:    true,
// 			// faked time
// 			modTime: time.Now(),
// 		}, nil
// 	}
//
// 	v := it.Value()
// 	defer v.Free()
// 	fi.fileName = key
// 	err = fi.UnmarshalBinary(v.Data())
// 	return
// }
//
// // count -1 means unlimited
// // listN list count entries under directory keyPrefix, not including itself
// func (db *rocksDBIndex) ListN(keyPrefix string, count int) ([]string, error) {
// 	if db.closed {
// 		return nil, ErrRocksDBClosed
// 	}
// 	defer func(before time.Time) {
// 		DiskOperationDuration.With(prometheus.Labels{"operation_type": "RocksDB-ListN"}).Observe(time.Since(before).Seconds())
// 	}(time.Now())
//
// 	// keyPrefix = pathJoin(keyPrefix)
//
// 	// list from directory cache
// 	if status := db.directoryStatus.Load().(string); status == directoryStatusWorking {
// 		if seg := db.segment(keyPrefix); len(seg) < db.cacheLevel && db.directory != nil {
// 			return db.directory.list(seg, count), nil
// 		}
// 	}
//
// 	ro := gorocksdb.NewDefaultReadOptions()
// 	ro.SetFillCache(false)
// 	defer ro.Destroy()
// 	it := db.db.NewIterator(ro)
// 	defer it.Close()
//
// 	var entryMap map[string]struct{}
// 	if count <= 0 {
// 		entryMap = make(map[string]struct{})
// 	} else {
// 		entryMap = make(map[string]struct{}, count)
// 	}
//
// 	addToEntries := func(entry string) {
// 		if _, ok := entryMap[entry]; ok {
// 			return
// 		}
// 		entryMap[entry] = struct{}{}
// 		count--
// 	}
//
// 	it.Seek([]byte(keyPrefix))
// 	// the directory not found
// 	if !it.Valid() {
// 		return nil, os.ErrNotExist
// 	}
//
// 	for count != 0 {
// 		if db.closed {
// 			return nil, ErrRocksDBClosed
// 		}
// 		if !it.ValidForPrefix([]byte(keyPrefix)) {
// 			break
// 		}
// 		key := it.Key()
// 		entry := subDir(string(key.Data()), keyPrefix)
// 		if entry == "" {
// 			key.Free()
// 			it.Next()
// 			continue
// 		}
// 		addToEntries(entry)
//
// 		key.Free()
//
// 		for {
// 			if db.closed {
// 				return nil, ErrRocksDBClosed
// 			}
// 			it.Next()
// 			if !it.ValidForPrefix([]byte(pathJoin(keyPrefix, entry))) {
// 				break
// 			}
// 		}
// 	}
//
// 	if err := it.Err(); err != nil {
// 		return nil, err
// 	}
// 	entries := make([]string, 0, len(entryMap))
// 	for k, _ := range entryMap {
// 		entries = append(entries, k)
// 	}
// 	return entries, nil
// }
//
// func (db *rocksDBIndex) ScanAll(ctx context.Context, filter func(s string) bool) (chan FileInfo, chan error) {
// 	ch := make(chan FileInfo, 1000)
// 	ech := make(chan error, 1)
// 	go func(ctx context.Context) {
// 		defer close(ch)
// 		defer close(ech)
// 		ro := gorocksdb.NewDefaultReadOptions()
// 		ro.SetFillCache(false)
// 		defer ro.Destroy()
// 		it := db.db.NewIterator(ro)
// 		defer it.Close()
//
// 		it.SeekToFirst()
// 		breaks := false
//
// 		for it = it; it.Valid() && !breaks; it.Next() {
// 			key := it.Key()
// 			if filter(string(key.Data())) {
// 				var fi FileInfo
// 				fi.fileName = string(key.Data())
// 				value := it.Value()
// 				err := fi.UnmarshalBinary(value.Data())
// 				if err != nil {
// 					select {
// 					case ech <- err:
// 					case <-ctx.Done():
// 						breaks = true
// 					}
// 				} else {
// 					select {
// 					case ch <- fi:
// 					case <-ctx.Done():
// 						breaks = true
// 					}
// 				}
// 				value.Free()
// 			}
// 			key.Free()
// 		}
// 		if err := it.Err(); err != nil {
// 			ech <- err
// 			return
// 		}
// 	}(ctx)
// 	return ch, ech
// }
//
// func (db *rocksDBIndex) Close() error {
// 	if db.closed {
// 		return nil
// 	}
// 	db.wg.Wait()
// 	db.db.Close()
// 	db.ro.Destroy()
// 	db.wo.Destroy()
// 	db.opts.Destroy()
// 	db.closed = true
// 	return nil
// }
//
// func (db *rocksDBIndex) Remove() (err error) {
// 	defer func(before time.Time) {
// 		DiskOperationDuration.With(prometheus.Labels{"operation_type": "Remove"}).Observe(time.Since(before).Seconds())
// 	}(time.Now())
// 	name := db.db.Name()
// 	err = multierr.Append(err, db.Close())
//
// 	opts := gorocksdb.NewDefaultOptions()
// 	defer opts.Destroy()
// 	err = multierr.Append(err, gorocksdb.DestroyDb(name, opts))
//
// 	err = multierr.Append(err, os.RemoveAll(path.Dir(strings.TrimRight(name, "/"))))
//
// 	if db.backupDir != "" {
// 		err = multierr.Append(err, os.RemoveAll(path.Dir(strings.TrimRight(db.backupDir, "/"))))
// 	}
// 	return err
// }
//
// // p1		p2	 	result
// // a/b/c 	        a/
// // a               a
// // a       a
// // a/b/c  	a       b/
// // aa/b/c 	a
// // a/b/c 	b
// // p1, p2 will never start with '/'
// func subDir(p1, p2 string) string {
// 	if p2 == "" {
// 		goto firstPart
// 	}
// 	if !strings.HasSuffix(p2, "/") {
// 		p2 += "/"
// 	}
// 	if !strings.HasPrefix(p1, p2) {
// 		return ""
// 	}
// 	p1 = strings.TrimPrefix(p1, p2)
// firstPart:
// 	p1 = strings.TrimPrefix(p1, "/")
// 	idx := strings.Index(p1, "/")
// 	if idx == -1 {
// 		return p1
// 	}
// 	return p1[:idx+1]
// }
