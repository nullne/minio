package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	fv "github.com/minio/minio/cmd/volume"
	"github.com/syndtr/goleveldb/leveldb"
	"gopkg.in/bufio.v1"
)

var globalFileVolumes = struct {
	volumes sync.Map

	// only path in init map can be inited
	init sync.Map
}{}

func addFileVolume(path string) error {
	// check existence
	_, err := getFileVolume(path)
	if err == nil {
		return nil
	}

	_, ok := globalFileVolumes.init.LoadOrStore(path, struct{}{})
	// someone is initializing, wait and check again
	if ok {
		time.Sleep(time.Second)
		return addFileVolume(path)
	}

	index, err := fv.NewRocksDBIndex(path, fv.RocksDBOptions{
		Root:        globalRocksDBRoot,
		BloomFilter: globalRocksDBBloomFilter,
	})
	vol, err := fv.NewVolume(context.Background(), path, index)
	globalFileVolumes.init.Delete(path)
	if err != nil {
		return err
	}

	globalFileVolumes.volumes.Store(path, vol)
	return nil
}

func getFileVolume(path string) (*fv.Volume, error) {
	vol, ok := globalFileVolumes.volumes.Load(path)
	if !ok {
		return nil, fmt.Errorf("file volume %s not found", path)
	}
	return vol.(*fv.Volume), nil
}

// @TODO check validaty of delete
func closeFileVolume() error {
	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
		value.(*fv.Volume).Close()
		globalFileVolumes.volumes.Delete(key)
		return true
	})
	return nil
}

func convertError(err error) error {
	switch err {
	case leveldb.ErrNotFound, os.ErrNotExist:
		return errFileNotFound
	default:
		return err
	}
}

func (s *posix) readAllFromFileVolume(volume, path string) (buf []byte, err error) {
	defer func() { err = convertError(err) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.ReadAll(path)
}

func (s *posix) readFileFromFileVolume(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (nn int64, err error) {
	defer func() { err = convertError(err) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return 0, err
	}
	if verifier == nil {
		return vol.ReadFile(path, offset, buffer)
	}

	bs, err := vol.ReadAll(path)
	if err != nil {
		return 0, err
	}
	r := bufio.NewBuffer(bs)

	bufp := s.pool.Get().(*[]byte)
	defer s.pool.Put(bufp)

	h := verifier.algorithm.New()
	if _, err = io.CopyBuffer(h, r, *bufp); err != nil {
		return 0, err
	}

	if n, err := io.ReadFull(r, buffer); err != nil {
		return int64(n), err
	}

	if _, err = h.Write(buffer); err != nil {
		return 0, err
	}

	if _, err = io.CopyBuffer(h, r, *bufp); err != nil {
		return 0, err
	}

	if !bytes.Equal(h.Sum(nil), verifier.sum) {
		return 0, hashMismatchError{hex.EncodeToString(verifier.sum), hex.EncodeToString(h.Sum(nil))}
	}

	return int64(len(buffer)), nil
}

func (s *posix) readFileStreamFromFileVolume(volume, path string, offset, length int64) (r io.ReadCloser, err error) {
	defer func() { err = convertError(err) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.ReadFileStream(path, offset, length)
}

func (s *posix) writeAllToFileVolume(volume, path string, buf []byte) error {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.WriteAll(path, int64(len(buf)), bufio.NewBuffer(buf))
}

func (s *posix) createFileToFileVolume(volume, path string, fileSize int64, r io.Reader) error {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.WriteAll(path, fileSize, r)
}

func (s *posix) deleteFromFileVolume(volume, path string) error {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.Delete(path)
}

func (s *posix) statFileFromFileVolume(volume, path string) (info FileInfo, err error) {
	defer func() { err = convertError(err) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return FileInfo{}, err
	}
	fi, err := vol.Stat(path)
	if err != nil {
		return FileInfo{}, err
	}
	if fi.IsDir() {
		return FileInfo{}, errFileNotFound
	}
	return FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		Mode:    fi.Mode(),
	}, nil
}

func (s *posix) statDirFromFileVolume(volume, path string) (volInfo VolInfo, err error) {
	defer func() { err = convertError(err) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return VolInfo{}, err
	}
	fi, err := vol.StatDir(path)
	if err != nil {
		return VolInfo{}, err
	}
	return VolInfo{
		Name:    fi.Name(),
		Created: fi.ModTime(),
	}, nil
}

func (s *posix) listDirFromFileVolume(volume, dirPath string, count int) (entries []string, err error) {
	defer func() { err = convertError(err) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}

	return vol.List(dirPath, count)
}

func (s *posix) appendFileToFileVolume(volume, path string, buf []byte) (err error) {
	return nil
}
