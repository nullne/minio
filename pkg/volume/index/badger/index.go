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
	"github.com/dgraph-io/badger/options"
	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
	"go.uber.org/multierr"
)

type badgerIndex struct {
	*badger.DB

	dir string

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
		DB:  db,
		dir: p,
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

	switch os.Getenv("MINIO_BADGER_TABLE_LOADING_MODE") {
	case "FileIO":
		opts.TableLoadingMode = options.FileIO
	case "MemoryMap":
		opts.TableLoadingMode = options.MemoryMap
	case "LoadToRAM":
		opts.TableLoadingMode = options.LoadToRAM
	default:
		opts.TableLoadingMode = options.LoadToRAM
	}

	switch os.Getenv("MINIO_BADGER_VALUE_LOADING_MODE") {
	case "FileIO":
		opts.ValueLogLoadingMode = options.FileIO
	case "MemoryMap":
		opts.ValueLogLoadingMode = options.MemoryMap
	case "LoadToRAM":
		opts.ValueLogLoadingMode = options.LoadToRAM
	default:
		opts.ValueLogLoadingMode = options.MemoryMap
	}
	// opts.Logger = nil

	// default values
	// opts.MaxTableSize = 64 << 20
	// opts.LevelSizeMultiplier = 10
	// opts.MaxLevels = 7
	// opts.ValueThreshold = 32
	// opts.NumMemtables = 5
	// opts.NumLevelZeroTables = 5
	// opts.NumLevelZeroTablesStall = 10
	// opts.LevelOneSize = 256 << 20
	// opts.ValueLogFileSize = 1 << 30
	// opts.ValueLogMaxEntries = 1000000
	// opts.NumCompactors = 3
	// opts.DoNotCompact = false

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

func (db *badgerIndex) Remove() (err error) {
	err = multierr.Append(err, db.DropAll())
	err = multierr.Append(err, db.Close())
	err = multierr.Append(err, os.RemoveAll(db.dir))
	return nil
}
