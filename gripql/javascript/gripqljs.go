package gripqljs

import (
	"embed"
)

//go:embed gripql.js
var js embed.FS

func Asset(path string) (string, error) {
	data, err := js.ReadFile(path)
	return string(data), err
}
