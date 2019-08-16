package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	fv "github.com/minio/minio/cmd/volume"
	"gopkg.in/bufio.v1"
)

var globalFileVolumes = struct {
	volumes sync.Map

	// only path in init map can be inited
	init sync.Map
}{}

func removeFileVolume(path string) *fv.Volume {
	vol, ok := globalFileVolumes.volumes.Load(path)
	if !ok {
		return nil
	}
	globalFileVolumes.volumes.Delete(path)
	return vol.(*fv.Volume)
}

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
		logger.Info("sleep to wait other to add volume %s", path)
		return addFileVolume(path)
	}

	vol, err := fv.NewVolume(context.Background(), path)
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

// @TODO check validity of delete
func closeFileVolume() error {
	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
		if err := value.(*fv.Volume).Close(); err != nil {
			logger.LogIf(context.Background(), err)
		}
		globalFileVolumes.volumes.Delete(key)
		logger.Info("volume %s has been closed gracefully", key)
		return true
	})
	return nil
}

func convertError(err error, directory bool) error {
	switch {
	case err == os.ErrNotExist:
		if directory {
			return errVolumeNotFound
		} else {
			return errFileNotFound
		}
	case isSysErrIO(err):
		return errFaultyDisk
	default:
		return err
	}
}

func (s *posix) readAllFromFileVolume(volume, path string) (buf []byte, err error) {
	defer func() { err = convertError(err, false) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.ReadAll(path)
}

func (s *posix) readFileFromFileVolume(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (nn int64, err error) {
	defer func() { err = convertError(err, false) }()

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
	if _, err = io.CopyBuffer(h, io.LimitReader(r, offset), *bufp); err != nil {
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
	defer func() { err = convertError(err, false) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.ReadFileStream(path, offset, length)
}

func (s *posix) writeAllToFileVolume(volume, path string, buf []byte) (err error) {
	defer func() { err = convertError(err, false) }()
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.WriteAll(path, int64(len(buf)), bufio.NewBuffer(buf))
}

func (s *posix) createFileToFileVolume(volume, path string, fileSize int64, r io.Reader) (err error) {
	defer func() { err = convertError(err, false) }()
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.WriteAll(path, fileSize, r)
}

func (s *posix) deleteFromFileVolume(volume, path string) (err error) {
	defer func() { err = convertError(err, false) }()
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.Delete(path)
}

func (s *posix) statFileFromFileVolume(volume, path string) (info FileInfo, err error) {
	defer func() { err = convertError(err, false) }()

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
	defer func() { err = convertError(err, true) }()

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
	defer func() { err = convertError(err, true) }()

	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}

	return vol.List(dirPath, count)
}

func (s *posix) appendFileToFileVolume(volume, path string, buf []byte) (err error) {
	return errors.New("not implemented")
}

func (s *posix) deleteVolFromFileVolume(volume string) error {
	p := filepath.Join(s.diskPath, volume)
	vol := removeFileVolume(p)
	if vol == nil {
		return nil
	}
	if err := vol.Remove(); err != nil {
		switch {
		case os.IsPermission(err):
			return errDiskAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}
	return nil
}

func (s *posix) makeDirFromFileVolume(volume, path string) error {
	vol, err := getFileVolume(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.MakeDir(path)
}

func (s *posix) renameFileFromFileVolume(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	defer func() {
		err = convertError(err, false)
	}()
	var entries []string
	if !hasSuffix(srcPath, slashSeparator) {
		entries = []string{""}
	} else {
		es, err := s.ListDir(dstVolume, dstPath, 1)
		if err != nil && err != errVolumeNotFound {
			return err
		}
		if len(es) != 0 {
			return errFileAccessDenied
		}
		entries, err = s.ListDir(srcVolume, srcPath, -1)
		if err != nil {
			return err
		}
	}
	// empty directory
	if len(entries) == 0 {
		if err := s.MakeVol(path.Dir(pathJoin(dstVolume, dstPath))); err != nil && err != errVolumeExists {
			return err
		}
		return nil
	}
	for _, e := range entries {
		if hasSuffix(e, slashSeparator) {
			if err := s.renameFileFromFileVolume(srcVolume, pathJoin(srcPath, e), dstVolume, pathJoin(dstPath, e)); err != nil {
				return err
			}
			continue
		}
		fi, err := s.StatFile(srcVolume, pathJoin(srcPath, e))
		if err != nil {
			return err
		}
		// create dir before write to it
		if isMinioMetaBucketName(dstVolume) {
			if err := s.MakeVol(path.Dir(pathJoin(dstVolume, dstPath, e))); err != nil && err != errVolumeExists {
				return err
			}
		}
		file, err := s.ReadFileStream(srcVolume, pathJoin(srcPath, e), 0, fi.Size)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := s.CreateFile(dstVolume, pathJoin(dstPath, e), fi.Size, file); err != nil {
			return err
		}
	}

	return nil
}
