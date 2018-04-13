package test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	e := m.Run()
	files, err := filepath.Glob("test.db.*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.RemoveAll(f); err != nil {
			panic(err)
		}
	}
	os.Exit(e)
}
