// +build darwin dragonfly freebsd linux netbsd openbsd

package volume

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestFileLock(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	defer os.RemoveAll(dir)
	p := path.Join(dir, "LOCK")
	fl, err := newFileLock(p, false)
	if err != nil {
		t.Fatal(err)
	}
	defer fl.release()
	if _, err := newFileLock(p, false); err == nil {
		t.Fatal("should not get lock")
	}
}
