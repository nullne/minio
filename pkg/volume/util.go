package volume

import (
	"os"
	"path"
	"path/filepath"
	"strings"
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

func trimRightSlash(s string) string {
	if s == "/" {
		return s
	}
	return strings.TrimRight(s, "/")
}

func isDirectory(s string) bool {
	return strings.HasSuffix(s, slashSeperator)
}

// PathJoin - like path.Join() but retains trailing "/" of the last element
func PathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if strings.HasSuffix(elem[len(elem)-1], slashSeperator) {
			trailingSlash = slashSeperator
		}
	}
	ps := path.Join(elem...)
	if ps == slashSeperator {
		return ps
	}
	return ps + trailingSlash
}
