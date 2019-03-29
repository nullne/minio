package volume

import (
	"sort"
	"strings"
	"testing"
)

func TestPathTrie(t *testing.T) {
	segment := func(s string) []string {
		if s == "" {
			return nil
		}
		return strings.Split(s, "/")
	}

	root := newPathTrie()

	cases := []struct {
		fn       string
		path     string
		expected []string
	}{
		{"list", "", []string{}},
		{"list", "a/b", []string{}},
		{"delete", "", []string{}},
		{"delete", "a/b", []string{}},
		{"put", "a/b", []string{}},
		{"list", "", []string{"a"}},
		{"list", "a", []string{"b"}},
		{"list", "a/b", []string{}},
		{"put", "a/c", []string{}},
		{"list", "a", []string{"b", "c"}},
		{"list", "", []string{"a"}},
		{"delete", "a/b", []string{}},
		{"list", "a", []string{"c"}},
		{"delete", "a/c", []string{}},
		{"list", "a", []string{}},
		{"list", "", []string{}},
	}

	for _, tc := range cases {
		switch tc.fn {
		case "list":
			ls := root.list(segment(tc.path), -1)
			sort.Strings(ls)
			if len(ls) != len(tc.expected) {
				t.Errorf("got %v, wanna %v", ls, tc.expected)
				continue
			}
			for i, v := range ls {
				if v != tc.expected[i] {
					t.Errorf("got %v, wanna %v", ls, tc.expected)
					break
				}
			}
		case "put":
			root.put(segment(tc.path))
		case "delete":
			root.delete(segment(tc.path))
		}
	}
}
