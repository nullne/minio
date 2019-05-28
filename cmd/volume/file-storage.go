// +build !linux

package volume

import "os"

// Fallocate is not POSIX and not supported under Windows
// Always return successful
func Fallocate(fd int, offset int64, len int64) error {
	return nil
}

func DirectReadOnlyOpen(name string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY, perm)
}
