package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"plugin"

	"github.com/minio/minio/pkg/volume/interfaces"
	bufio "gopkg.in/bufio.v1"
)

func main() {
	p, err := plugin.Open("/tmp/volume-plugin.so")
	if err != nil {
		panic(err)
	}
	f, err := p.Lookup("NewVolume")
	if err != nil {
		// t.Fatal(err)
		panic(err)
	}
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := f.(func(context.Context, string) (interfaces.Volume, error))(context.Background(), dir)
	if err != nil {
		// t.Fatal(err)
		panic(err)
	}
	defer v.Close()
	key := "test-key"
	size := int64(1024)
	data := make([]byte, size)
	rand.Read(data)
	r := bufio.NewBuffer(data)
	if err := v.WriteAll(key, size, r); err != nil {
		// t.Fatal(err)
		panic(err)
	}
	bs, err := v.ReadAll(key)
	if err != nil {
		// t.Fatal(err)
		panic(err)
	}
	if !bytes.Equal(data, bs) {
		// t.Errorf("bytes not equal, wanna: %v, got: %v", len(data), len(bs))
		panic(err)
	}

	if err := v.Delete(key); err != nil {
		// t.Error(err)
		panic(err)
	}

	file3, err := v.ReadAll(key)
	if err == nil || file3 != nil {
		// t.Error("delete failed?")
		panic(err)
	}
}
