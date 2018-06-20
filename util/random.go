package util

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/ksuid"
)

func init() {
	rand.NewSource(time.Now().UnixNano())
}

// RandomString generates a random string of length n.
func RandomString(n int) string {
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

// UUID generates a k-sortable globally unique ID.
func UUID() string {
	return ksuid.New().String()
}

// RandomPort returns a random port string between 10000 and 20000.
func RandomPort() string {
	min := 10000
	max := 40000
	n := rand.Intn(max-min) + min
	return fmt.Sprintf("%d", n)
}
