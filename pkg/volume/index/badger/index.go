package badger

import (
	"bytes"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/dgraph-io/badger"
	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
)

type badgerIndex struct {
	*badger.DB

	closed uint32
	wg     sync.WaitGroup
}

func NewIndex(name string, options volume.IndexOptions) (volume.Index, error) {
	p := filepath.Join(options.Root, name, volume.IndexDir)
	if err := volume.MkdirIfNotExist(p); err != nil {
		return nil, err
	}

	opts := getOptions(p)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	idx := badgerIndex{
		DB: db,
	}

	return &idx, nil
}

func getOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)

	if s := os.Getenv("MINIO_BADGER_SYNC_WRITES"); s == "on" {
		opts.SyncWrites = true
	} else {
		opts.SyncWrites = false
	}

	// opts.NumVersionsToKeep = p.GetInt(badgerNumVersionsToKeep, 1)
	// opts.MaxTableSize = p.GetInt64(badgerMaxTableSize, 64<<20)
	// opts.LevelSizeMultiplier = p.GetInt(badgerLevelSizeMultiplier, 10)
	// opts.MaxLevels = p.GetInt(badgerMaxLevels, 7)
	// opts.ValueThreshold = p.GetInt(badgerValueThreshold, 32)
	// opts.NumMemtables = p.GetInt(badgerNumMemtables, 5)
	// opts.NumLevelZeroTables = p.GetInt(badgerNumLevelZeroTables, 5)
	// opts.NumLevelZeroTablesStall = p.GetInt(badgerNumLevelZeroTablesStall, 10)
	// opts.LevelOneSize = p.GetInt64(badgerLevelOneSize, 256<<20)
	// opts.ValueLogFileSize = p.GetInt64(badgerValueLogFileSize, 1<<30)
	// opts.ValueLogMaxEntries = uint32(p.GetUint64(badgerValueLogMaxEntries, 1000000))
	// opts.NumCompactors = p.GetInt(badgerNumCompactors, 3)
	// opts.DoNotCompact = p.GetBool(badgerDoNotCompact, false)
	// if b := p.GetString(badgerTableLoadingMode, "LoadToRAM"); len(b) > 0 {
	// 	if b == "FileIO" {
	// 		opts.TableLoadingMode = options.FileIO
	// 	} else if b == "LoadToRAM" {
	// 		opts.TableLoadingMode = options.LoadToRAM
	// 	} else if b == "MemoryMap" {
	// 		opts.TableLoadingMode = options.MemoryMap
	// 	}
	// }
	// if b := p.GetString(badgerValueLogLoadingMode, "MemoryMap"); len(b) > 0 {
	// 	if b == "FileIO" {
	// 		opts.ValueLogLoadingMode = options.FileIO
	// 	} else if b == "LoadToRAM" {
	// 		opts.ValueLogLoadingMode = options.LoadToRAM
	// 	} else if b == "MemoryMap" {
	// 		opts.ValueLogLoadingMode = options.MemoryMap
	// 	}
	// }

	return opts
}

func (db *badgerIndex) Get(key string) ([]byte, error) {
	var val []byte
	err := db.View(func(txn *badger.Txn) error {
		item, e := txn.Get([]byte(key))
		if e != nil {
			return e
		}
		val, e = item.ValueCopy(nil)
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, interfaces.ErrKeyNotExisted
		}
		return nil, err
	}
	return val, nil
}

func (db *badgerIndex) Set(key string, data []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), data)
		return err
	})
}

func (db *badgerIndex) Delete(keyPrefix string) error {
	var keys [][]byte
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(keyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			remaining := bytes.TrimPrefix(item.Key(), prefix)
			remaining = bytes.TrimPrefix(remaining, []byte{'/'})
			if idx := bytes.Index(remaining, []byte{'/'}); idx != -1 {
				// dont' delete not empty directory
				return syscall.ENOTEMPTY
			}
			// must copy or something strange happens
			keys = append(keys, item.KeyCopy(nil))

		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return interfaces.ErrNotExisted
	}
	return db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *badgerIndex) ListN(keyPrefix, leaf string, count int) ([]string, error) {
	var entries []string
	if count > 0 {
		entries = make([]string, 0, count)
	}

	prefix := []byte(keyPrefix)
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(prefix)
		if !it.Valid() {
			return interfaces.ErrNotExisted
		}

		for count != 0 {
			if !it.ValidForPrefix(prefix) {
				break
			}
			item := it.Item()
			k := item.Key()
			skey := string(k)
			entry := volume.SubDir(skey, keyPrefix)
			if entry == "" {
				it.Next()
				continue
			}
			if leaf != "" {
				if _, err := db.Get(path.Join(keyPrefix, entry, leaf)); err == nil {
					entry = strings.TrimRight(entry, "/")
				} else if err != nil && err != interfaces.ErrKeyNotExisted {
					return err
				}
			}
			entries = append(entries, entry)
			count--
			ignorePrefix := []byte(volume.PathJoin(keyPrefix, entry, "/"))
			for {
				it.Next()
				if !it.ValidForPrefix(ignorePrefix) {
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (db *badgerIndex) Close() error {
	return db.DB.Close()
}

func (db *badgerIndex) Remove() error {
	return nil
}
