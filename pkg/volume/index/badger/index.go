package badger

import (
	"bytes"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/dgraph-io/badger"
	bg "github.com/dgraph-io/badger"
	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
)

type badgerIndex struct {
	*bg.DB

	closed uint32
	wg     sync.WaitGroup
}

func NewIndex(name string, options volume.IndexOptions) (volume.Index, error) {
	p := filepath.Join(options.Root, name, volume.IndexDir)
	if err := volume.MkdirIfNotExist(p); err != nil {
		return nil, err
	}

	opts := bg.DefaultOptions(p)
	db, err := bg.Open(opts)
	if err != nil {
		return nil, err
	}
	idx := badgerIndex{
		DB: db,
	}

	return &idx, nil
}

func (db *badgerIndex) Get(key string) ([]byte, error) {
	var val []byte
	err := db.View(func(txn *bg.Txn) error {
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
		if err == bg.ErrKeyNotFound {
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
	err := db.View(func(txn *bg.Txn) error {
		opts := bg.DefaultIteratorOptions
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
	err := db.View(func(txn *bg.Txn) error {
		opts := bg.DefaultIteratorOptions
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
