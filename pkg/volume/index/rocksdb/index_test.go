package rocksdb

import (
	"fmt"
	"strings"
	"testing"
)

func TestSubDir(t *testing.T) {
	cases := []struct {
		p1  string
		p2  string
		res string
	}{
		{"a/b/c", "", "a/"},
		{"a", "", "a"},
		{"a", "a", ""},
		{"a/b/c", "a", "b/"},
		{"aa/b/c", "a", ""},
		{"a/b/c", "b", ""},
		{"a/b/c", "a/", "b/"},
	}

	for _, c := range cases {
		r := subDir(c.p1, c.p2)
		if r != c.res {
			t.Errorf("p1: %s, p2: %s, wanna: %s, got: %s", c.p1, c.p2, c.res, r)
		}
	}
}

func TestRandomPickFromTimeRange(t *testing.T) {
	cases := []struct {
		p   string
		err error
	}{
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
		{"15:30:00-16:30:00", nil},
	}

	for _, c := range cases {
		ss := strings.Split(c.p, "-")
		d, err := randomPickFromTimeRange(ss[0], ss[1])
		if err != c.err {
			t.Errorf("range: %s, wanna: %v, got: %v", c.p, c.err, err)
		}
		// check the result manully
		fmt.Println(d)
	}
}
