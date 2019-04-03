package cmd

import (
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	fv "github.com/nullne/didactic-couscous/volume"
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

	vol, err := fv.NewVolume(path, "hay")
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

func (s *posix) readAllFromFileVolume(volume, path string) (buf []byte, err error) {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.Get(path)
}

func (s *posix) readFileFromFileVolume(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (int64, error) {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return 0, err
	}
	bs, err := vol.Get(path)
	if err != nil {
		return 0, err
	}
	return int64(len(bs)) - offset, nil
}

func (s *posix) readFileStreamFromFileVolume(volume, path string, offset, length int64) (io.ReadCloser, error) {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	bs, err := vol.Get(path)
	if err != nil {
		return nil, err
	}
	file := bufio.NewBuffer(bs[offset:])
	return ioutil.NopCloser(io.LimitReader(file, length)), nil
}

func (s *posix) writeAllToFileVolume(volume, path string, buf []byte) error {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	_, err = vol.Put(path, uint64(len(buf)), bufio.NewBuffer(buf))
	return err
}

func (s *posix) createFileToFileVolume(volume, path string, fileSize int64, r io.Reader) error {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	n, err := vol.Put(path, uint64(fileSize), r)
	if err != nil {
		return err
	}
	if int64(n) < fileSize {
		return errLessData
	}
	if int64(n) > fileSize {
		return errMoreData
	}
	return nil
}
