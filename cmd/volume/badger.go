package volume

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
)

type badgerIndex struct {
	db *badger.DB
	ch chan brequest
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
		db: db,
		ch: make(chan brequest),
	}
	go b.batchWrite()
	return &b, nil
}

type brequest struct {
	key  []byte
	data []byte
	resp chan error
}

func (b *badgerIndex) batchWrite() {
	wb := b.db.NewWriteBatch()
	ticker := time.NewTicker(10 * time.Millisecond)
	count := 0

	for {
		select {
		case <-ticker.C:
		case req := <-b.ch:
			req.resp <- wb.Set(req.key, req.data, 0)
			if count < 100 {
				continue
			}
		}
		go func(wb *badger.WriteBatch) {
			if err := wb.Flush(); err != nil {
				fmt.Println(err)
			}
		}(wb)
		wb = b.db.NewWriteBatch()
		count = 0
	}
	// for req := range b.ch {
	// 	req.resp <- wb.Set(req.key, req.data, 0)
	// 	select {
	// 	case <-ticker.C:
	// 	default:
	// 		if count < 100 {
	// 			continue
	// 		}
	// 	}
	// 	if err := wb.Flush(); err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	fmt.Println("************")
	// 	wb = b.db.NewWriteBatch()
	// 	count = 0
	// }
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
	// b.batch.Set([]byte(key), data, 0)
	req := brequest{
		key:  []byte(key),
		data: data,
		resp: make(chan error),
	}
	b.ch <- req
	return <-req.resp
	// return b.db.Update(func(txn *badger.Txn) error {
	// 	err := txn.Set([]byte(key), data)
	// 	return err
	// })
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
	return b.db.Close()
}
