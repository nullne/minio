package volume

import "testing"

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
