package cmd

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"plugin"

	"github.com/minio/minio/pkg/volume/interfaces"
	"gopkg.in/bufio.v1"
)

func initGlobalFileVolume() (interfaces.Volumes, error) {
	pluginPath := os.Getenv("MINIO_FILE_VOLUME_PLUGIN_PATH")
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}
	newVolumes, err := p.Lookup("NewVolumes")
	if err != nil {
		return nil, err
	}
	vs := newVolumes.(func() interfaces.Volumes)()
	return vs, nil
}

func convertError(err error, directory bool) error {
	switch {
	case err == interfaces.ErrIsNotReguler:
		return errIsNotRegular
	case err == os.ErrNotExist || err == interfaces.ErrNotExisted || err == interfaces.ErrKeyNotExisted:
		if directory {
			return errVolumeNotFound
		} else {
			return errFileNotFound
		}
	case isSysErrIO(err):
		return errFaultyDisk
	case err == interfaces.ErrNotExisted:
		if directory {
			return errVolumeExists
		} else {
			return err
		}
	default:
		return err
	}
}

func (s *posix) mkdirFromFileVolume(volume, path string) (err error) {
	defer func() { err = convertError(err, true) }()
	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.Mkdir(path)
}

func (s *posix) statDirFromFileVolume(volume, path string) (vi VolInfo, err error) {
	defer func() { err = convertError(err, true) }()

	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return vi, err
	}
	fi, err := vol.Stat(path)
	if err != nil {
		return vi, err
	}
	// @TODO return which error
	// if !fi.IsDir() {
	// }
	return VolInfo{
		Name:    fi.Name(),
		Created: fi.ModTime(),
	}, nil
}

func (s *posix) deleteVolFromFileVolume(volume string) error {
	p := filepath.Join(s.diskPath, volume)
	err := globalFileVolumes.Remove(p)
	if err == nil {
		return nil
	}

	switch {
	case os.IsPermission(err):
		return errDiskAccessDenied
	case isSysErrIO(err):
		return errFaultyDisk
	default:
		return err
	}
}

func (s *posix) listDirFromFileVolume(volume, dirPath string, count int) (entries []string, err error) {
	defer func() { err = convertError(err, true) }()

	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}

	return vol.List(dirPath, count)
}

func (s *posix) readAllFromFileVolume(volume, path string) (buf []byte, err error) {
	defer func() { err = convertError(err, false) }()

	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.ReadAll(path)
}

func (s *posix) readFileFromFileVolume(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (nn int64, err error) {
	defer func() { err = convertError(err, false) }()

	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
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
		return 0, HashMismatchError{hex.EncodeToString(verifier.sum), hex.EncodeToString(h.Sum(nil))}
	}

	return int64(len(buffer)), nil
}

func (s *posix) readFileStreamFromFileVolume(volume, path string, offset, length int64) (r io.ReadCloser, err error) {
	defer func() { err = convertError(err, false) }()

	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return nil, err
	}
	return vol.ReadFileStream(path, offset, length)
}

func (s *posix) createFileToFileVolume(volume, path string, fileSize int64, r io.Reader) error {
	bs := make([]byte, fileSize)
	_, err := io.ReadFull(r, bs)
	if err == nil {
		return s.writeAllToFileVolume(volume, path, bs)
	}
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return errLessData
	}
	return err
}

func (s *posix) writeAllToFileVolume(volume, path string, buf []byte) (err error) {
	defer func() { err = convertError(err, false) }()
	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.WriteAll(path, buf)
}

func (s *posix) appendFileToFileVolume(volume, path string, buf []byte) (err error) {
	return errors.New("not implemented")
}

func (s *posix) statFileFromFileVolume(volume, path string) (info FileInfo, err error) {
	defer func() { err = convertError(err, false) }()

	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
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

func (s *posix) deleteFromFileVolume(volume, path string) (err error) {
	defer func() { err = convertError(err, false) }()
	vol, err := globalFileVolumes.Get(filepath.Join(s.diskPath, volume))
	if err != nil {
		return err
	}
	return vol.Delete(path)
}

// @TODO should implement other type of rename
func (s *posix) renameFileFromFileVolume(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	defer func() {
		err = convertError(err, false)
	}()
	var entries []string
	if !hasSuffix(srcPath, SlashSeparator) {
		entries = []string{""}
	} else {
		es, err := s.ListDir(dstVolume, dstPath, 1, "")
		if err != nil && !(err == errVolumeNotFound || err == errFileNotFound) {
			return err
		}
		if len(es) != 0 {
			return errFileAccessDenied
		}
		entries, err = s.ListDir(srcVolume, srcPath, -1, "")
		if err != nil {
			return err
		}
	}
	// empty directory
	if len(entries) == 0 {
		if err := s.MakeVol(path.Dir(pathJoin(dstVolume, dstPath))); err != nil && err != errVolumeExists {
			return err
		}
		return s.DeleteFile(srcVolume, srcPath)
	}
	for _, e := range entries {
		if hasSuffix(e, SlashSeparator) {
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
		// make sure it's absent
		if err := s.DeleteFile(dstVolume, pathJoin(dstPath, e)); err != nil && err != errFileNotFound {
			return err
		}
		if err := s.CreateFile(dstVolume, pathJoin(dstPath, e), fi.Size, file); err != nil {
			return err
		}
		if err := s.DeleteFile(srcVolume, pathJoin(srcPath, e)); err != nil {
			return err
		}
	}

	return nil
}
