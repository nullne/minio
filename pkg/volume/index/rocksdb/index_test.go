package rocksdb

import (
	"io/ioutil"
	"syscall"
	"testing"

	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
)

func setupRocksdb() (volume.Index, error) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	return NewIndex(dir, volume.IndexOptions{})
}

func TestDelete(t *testing.T) {
	vol, err := setupRocksdb()
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Remove()

	for _, key := range []string{
		"bar/",
		"demo/part.1",
		"demo/xl.json",
		"foo/demo/part.1",
		"foo/demo/xl.json",
	} {
		if err := vol.Set(key, []byte{}); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		key           string
		expectedError error
	}{
		{"demo", nil},
		{"foo", syscall.ENOTEMPTY},
		{"notExist", interfaces.ErrNotExisted},
	}

	for i, c := range cases {
		err = vol.Delete(c.key)
		if err != c.expectedError {
			t.Errorf("case %d: expect error %v, got %v", i, c.expectedError, err)
		}
	}
}

func TestListN(t *testing.T) {
	vol, err := setupRocksdb()
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Remove()

	for _, key := range []string{
		"bar/",
		"demo/part.1",
		"demo/xl.json",
		"foo/demo/part.1",
		"foo/demo/xl.json",
	} {
		if err := vol.Set(key, []byte{}); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		key            string
		leaf           string
		count          int
		expectedOutput []string
		expectedError  error
	}{
		{"", "xl.json", -1, []string{"bar/", "demo", "foo/"}, nil},
		{"", "", -1, []string{"bar/", "demo/", "foo/"}, nil},
		{"", "", 2, []string{"bar/", "demo/"}, nil},
		{"", "", 1, []string{"bar/"}, nil},
		{"foo", "xl.json", -1, []string{"demo"}, nil},
		{"notExist", "", 1, []string{}, interfaces.ErrNotExisted},
	}

	for i, c := range cases {
		es, err := vol.ListN(c.key, c.leaf, c.count)
		if err != c.expectedError {
			t.Errorf("case %d: expect error %v, got %v", i, c.expectedError, err)
		}
		if len(es) != len(c.expectedOutput) {
			t.Errorf("case %d: expect entry num %v, got %v", i, len(c.expectedOutput), len(es))
		}
		for i := 0; i < len(es); i++ {
			if es[i] != c.expectedOutput[i] {
				t.Errorf("case %d: expect entries %v, got %v", i, c.expectedOutput, es)
			}
		}
	}
}
