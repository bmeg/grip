package tpath

import "testing"

func TestPathNormalize(t *testing.T) {

	pairs := [][]string{
		{"_label", "$_current._label"},
		{"name", "$_current.name"},
		{"$.name", "$_current.name"},
		{"$name", "$name"},
		{"$a.name", "$a.name"},
	}

	for _, p := range pairs {
		o := NormalizePath(p[0])
		if o != p[1] {
			t.Errorf("Normalize %s error: %s != %s", p[0], o, p[1])
		}
	}

}
