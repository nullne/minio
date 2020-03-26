// +build linux

package volume

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Fallocate uses the linux Fallocate syscall, which helps us to be
// sure that subsequent writes on a file just created will not fail,
// in addition, file allocation will be contigous on the disk
func Fallocate(fd int, offset int64, len int64) error {
	// No need to attempt fallocate for 0 length.
	if len == 0 {
		return nil
	}
	// Don't extend size of file even if offset + len is
	// greater than file size from <bits/fcntl-linux.h>.
	return syscall.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, offset, len)
}

func DirectReadOnlyOpen(name string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY, perm)
	// return os.OpenFile(name, os.O_RDONLY|syscall.O_DIRECT, perm)
}
