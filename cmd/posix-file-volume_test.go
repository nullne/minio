package cmd

import (
	"bytes"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"gopkg.in/bufio.v1"
)

func TestFileVolume(t *testing.T) {
	dir, _ := ioutil.TempDir("", "nullne_test_volume_")
	defer os.RemoveAll(dir)
	if err := addFileVolume(dir); err != nil {
		t.Error(err)
		return
	}
	_, err := getFileVolume("nono")
	if err == nil {
		t.Errorf("nono should not exist")
		return
	}
	fv, err := getFileVolume(dir)
	if err != nil {
		t.Error(err)
		return
	}
	bs1 := []byte("fooo")
	_, err = fv.Put("key", 1, bufio.NewBuffer(bs1))
	if err != nil {
		t.Error(err)
		return
	}
	bs2, err := fv.Get("key")
	if err != nil {
		t.Error(err)
		return
	}
	if !bytes.Equal(bs1, bs2) {
		t.Errorf("not equal")
	}
	closeFileVolume()
}

func TestFileVolumeConcurrent(t *testing.T) {
	dir, _ := ioutil.TempDir("", "nullne_test_volume_")
	defer os.RemoveAll(dir)
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			if err := addFileVolume(dir); err != nil {
				t.Error(err)
				return
			}
		}(&wg)
	}
	wg.Wait()
	fv, err := getFileVolume(dir)
	if err != nil {
		t.Error(err)
		return
	}
	bs1 := []byte("fooo")
	_, err = fv.Put("key", 1, bufio.NewBuffer(bs1))
	if err != nil {
		t.Error(err)
		return
	}
	bs2, err := fv.Get("key")
	if err != nil {
		t.Error(err)
		return
	}
	if !bytes.Equal(bs1, bs2) {
		t.Errorf("not equal")
	}
	closeFileVolume()
}
