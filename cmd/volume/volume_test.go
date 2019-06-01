package volume_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio/cmd/volume"
	"gopkg.in/bufio.v1"
)

func TestVolumeAndFileConcurrently(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	index, err := volume.NewRocksDBIndex(dir, volume.RocksDBOptions{})
	v, err := volume.NewVolume(context.Background(), dir, index)
	if err != nil {
		t.Error(err)
		return
	}
	defer v.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sizes := make([]int64, 10)
			for i := 0; i < len(sizes); i++ {
				sizes[i] = mrand.Int63n(1024 * 100)
			}
			for i, size := range sizes {
				data := make([]byte, size)
				rand.Read(data)
				r := bufio.NewBuffer(data)

				key := fmt.Sprintf("%d-%d", i, mrand.Int())
				err := v.WriteAll(key, size, r)
				if err != nil {
					t.Error(i, err)
					continue
				}

				bs, err := v.ReadAll(key)
				if err != nil {
					t.Error(i, err)
					continue
				}

				if !bytes.Equal(data, bs) {
					t.Errorf("%v: bytes not equal, wanna: %v, got: %v", i, len(data), len(bs))
				}

				err = v.Delete(key)
				if err != nil {
					t.Error(err)
					continue
				}

				file3, err := v.ReadAll(key)
				if err == nil || file3 != nil {
					t.Error("delete failed?")
				}
			}
		}()
	}
	wg.Wait()
}

func TestVolumeAndFile(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	for idx := 0; idx < 3; idx++ {
		func() {
			index, err := volume.NewRocksDBIndex(dir, volume.RocksDBOptions{})
			v, err := volume.NewVolume(context.Background(), dir, index)
			if err != nil {
				t.Error(err)
				return
			}
			defer v.Close()

			_, err = volume.NewVolume(context.Background(), dir, index)
			// _, err = volume.NewVolume(context.Background(), dir)
			if err == nil {
				t.Error("should not init twice ")
			}

			for i, size := range []int64{1, 100, 1024, 1024 * 1024, 1024 * 1024 * 10, 1024, 100, 1} {
				data := make([]byte, size)
				rand.Read(data)
				r := bufio.NewBuffer(data)

				key := fmt.Sprint(i)
				err := v.WriteAll(key, size, r)
				if err != nil {
					t.Error(i, err)
					continue
				}

				bs, err := v.ReadAll(key)
				if err != nil {
					t.Error(i, err)
					continue
				}

				if !bytes.Equal(data, bs) {
					t.Error(i, "bytes not equal")
				}

				err = v.Delete(key)
				if err != nil {
					t.Error(err)
				}

				file3, err := v.ReadAll(key)
				if err == nil || file3 != nil {
					t.Fatal("delete failed?")
				}
			}
		}()
	}
}

func TestVolumeReadFile(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	index, err := volume.NewRocksDBIndex(dir, volume.RocksDBOptions{})
	v, err := volume.NewVolume(context.Background(), dir, index)
	if err != nil {
		t.Error(err)
		return
	}
	// defer v.Close()

	key := "key"
	data := []byte("0123456789")
	r := bufio.NewBuffer(data)
	if err := v.WriteAll(key, int64(len(data)), r); err != nil {
		t.Error(err)
		return
	}

	r = bufio.NewBuffer(data)
	if err := v.WriteAll("another KEY", int64(len(data)), r); err != nil {
		t.Error(err)
		return
	}

	data2, err := v.ReadAll(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, data2) {
		t.Errorf("not equal")
	}

	data3 := make([]byte, 5)
	n, err := v.ReadFile(key, 1, data3)
	if err != nil {
		t.Error(err)
	}
	if n != 5 {
		t.Errorf("not equal")
	}
	if !bytes.Equal(data[1:6], data3) {
		t.Errorf("not equal")
	}

	rr, err := v.ReadFileStream(key, 5, 6)
	if err != nil {
		t.Error(err)
		return
	}
	data4, err := ioutil.ReadAll(rr)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(data[5:], data4) {
		t.Errorf("not equal")
	}

	if err := v.Remove(); err != nil {
		t.Error(err)
	}
}

func TestVolumeList(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	index, err := volume.NewRocksDBIndex(dir, volume.RocksDBOptions{})
	v, err := volume.NewVolume(context.Background(), dir, index)
	if err != nil {
		t.Error(err)
		return
	}
	defer v.Close()

	for i := 30; i > 10; i-- {
		r := bufio.NewBuffer([]byte("nice"))
		key := strings.Join(strings.Split(fmt.Sprint(i), ""), "/") + ".go"

		if err := v.WriteAll(key, 4, r); err != nil {
			t.Error(err)
			return
		}
	}
	entries, err := v.List("", -1)
	if err != nil {
		t.Error(err)
		return
	}
	if strings.Join(entries, ",") != "1/,2/,3/" {
		t.Error("wrong list entries", entries)
	}

	entries, err = v.List("1", -1)
	if err != nil {
		t.Error(err)
		return
	}
	if strings.Join(entries, " ") != "1.go 2.go 3.go 4.go 5.go 6.go 7.go 8.go 9.go" {
		t.Error("wrong list entries", entries)
	}
}

func TestDirOperation(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	index, err := volume.NewRocksDBIndex(dir, volume.RocksDBOptions{})
	v, err := volume.NewVolume(context.Background(), dir, index)
	if err != nil {
		t.Error(err)
		return
	}
	defer v.Close()

	paths := []string{
		"/",
		"/a/b/c/",
		"/a/b",
	}
	for _, p := range paths {
		if err := v.MakeDir(p); err != nil {
			t.Error(err)
			return
		}
		fi, err := v.StatDir(p)
		if err != nil {
			t.Error(err)
			return
		}
		if !fi.IsDir() {
			t.Error("not a directory")
			return
		}
		entries, err := v.List(p, 1)
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("---------------------", entries)
		if err := v.Delete(p); err != nil {
			t.Error(err)
			return
		}
		if _, err := v.StatDir(p); err != os.ErrNotExist {
			t.Error("should not exist")
			return
		}
	}
}
