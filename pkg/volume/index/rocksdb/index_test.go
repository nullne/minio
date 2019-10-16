package rocksdb

import (
	"io/ioutil"
	"testing"

	"github.com/minio/minio/pkg/volume"
)

func setupRocksdb() (volume.Index, error) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	return NewIndex(dir, volume.IndexOptions{})
}

func TestListN(t *testing.T) {
	vol, err := setupRocksdb()
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Remove()

	for _, key := range []string{
		"demo/part.1",
		"demo/xl.json",
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
		{"", "xl.json", -1, []string{"demo"}, nil},
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

// func TestRandomPickFromTimeRange(t *testing.T) {
// 	cases := []struct {
// 		p   string
// 		err error
// 	}{
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 		{"15:30:00-16:30:00", nil},
// 	}
//
// 	for _, c := range cases {
// 		ss := strings.Split(c.p, "-")
// 		d, err := randomPickFromTimeRange(ss[0], ss[1])
// 		if err != c.err {
// 			t.Errorf("range: %s, wanna: %v, got: %v", c.p, c.err, err)
// 		}
// 		// check the result manully
// 		fmt.Println(d)
// 	}
// }
