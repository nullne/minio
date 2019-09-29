package volume_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"plugin"
	"syscall"
	"testing"

	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
)

func newVolumeSetup() (func(context.Context, string) (interfaces.Volume, error), string, error) {
	dir, err := ioutil.TempDir("/tmp", "volume-")
	if err != nil {
		return nil, "", err
	}
	pluginPath := path.Join(dir, "plugin.so")
	cmd := exec.Command("go", "build", "-buildmode=plugin", fmt.Sprintf("-o=%s", pluginPath), "github.com/minio/minio/pkg/volume/plugin/rocksdb")
	if err := cmd.Run(); err != nil {
		return nil, "", err
	}
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, "", err
	}
	newVolume, err := p.Lookup("NewVolume")
	if err != nil {
		return nil, "", err
	}
	return newVolume.(func(context.Context, string) (interfaces.Volume, error)), dir, nil
}

func TestNewVolume(t *testing.T) {
	fn, dir, err := newVolumeSetup()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// v, err := fn(context.Background(), dir)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer v.Close()

	// Temporary dir name.
	tmpDirName := path.Join(dir, "dir")
	// Temporary file name.
	tmpFileName := path.Join(dir, "file")
	f, _ := os.Create(tmpFileName)
	f.Close()
	defer os.Remove(tmpFileName)

	// List of all tests for posix initialization.
	testCases := []struct {
		name string
		err  error
	}{
		// Validates input argument cannot be empty.
		{
			"",
			volume.ErrInvalidArgument,
		},
		// Validates if the directory does not exist and
		// gets automatically created.
		{
			tmpDirName,
			nil,
		},
		// Validates if the disk exists as file and returns error
		// not a directory.
		{
			tmpFileName,
			syscall.ENOTDIR,
		},
	}
	// Validate all test cases.
	for i, testCase := range testCases {
		// Initialize a new posix layer.
		v, err := fn(context.Background(), testCase.name)
		if err != testCase.err {
			t.Fatalf("TestNewVolume %d failed wanted: %s, got: %s", i+1, testCase.err, err)
		}
		if err != nil {
			continue
		}
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}
}
