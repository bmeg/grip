package kvbench

import (
	"math/rand"
)

var idRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randID() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = idRunes[rand.Intn(len(idRunes))]
	}
	return string(b)
}
