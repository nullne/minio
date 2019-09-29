package volume

import (
	"os"
	"path/filepath"
	"syscall"
)

func MkdirIfNotExist(p string) error {
	fi, err := os.Stat(p)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(p, 0755); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	if !fi.IsDir() {
		return syscall.ENOTDIR
	}
	return nil
}

func GetValidPath(path string) (string, error) {
	if path == "" {
		return path, ErrInvalidArgument
	}

	var err error
	// Disallow relative paths, figure out absolute paths.
	path, err = filepath.Abs(path)
	if err != nil {
		return path, err
	}
	return path, nil
}
