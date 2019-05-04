package volume

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger"
)

type badgerIndex struct {
	db    *badger.DB
	batch *badger.WriteBatch
}

func newBadgerIndex(dir string) (Index, error) {
	path := filepath.Join("/var/minio/badger", dir, "index")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	b := badgerIndex{
		db:    db,
		batch: db.NewWriteBatch(),
	}
	return &b, nil
}

func (b *badgerIndex) Get(key string) (fi FileInfo, err error) {
	var data []byte
	err = b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fi, err
	}
	fi.fileName = key
	if strings.HasSuffix(fi.fileName, "xl.json") {
		fi.data = data
		fi.size = uint64(len(data))
		// fi.modTime = time.Now()
		return
	}
	err = fi.UnmarshalBinary(data)
	return
}

func (b *badgerIndex) Set(key string, fi FileInfo) error {
	var data []byte
	if strings.HasSuffix(key, "xl.json") {
		data = fi.data
	} else {
		data = fi.MarshalBinary()
	}
	return b.batch.Set([]byte(key), data, 0)
}

func (b *badgerIndex) Delete(key string) error {
	return nil
}

func (b *badgerIndex) List(keyPrefix string) ([]string, error) {
	return nil, nil
}

func (b *badgerIndex) ListN(keyPrefix string, count int) ([]string, error) {
	return nil, nil
}

func (b *badgerIndex) Close() error {
	if err := b.batch.Flush(); err != nil {
		fmt.Println("failed to flush badger", err)
	}
	return b.db.Close()
}
