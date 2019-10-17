package volume_test

import (
	"flag"
	"testing"
)

var (
	fIndex = flag.String("index", "", "pick up which index to test file volume, available values is rocksdb")
)

func TestMain(t *testing.M) {
	flag.Parse()
	if *fIndex == "" {
		return
	}
}
