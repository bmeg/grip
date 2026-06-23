package util

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/ksuid"
)

// RandomString generates a random string of length n.
func RandomString(n int) string {
	rand.NewSource(time.Now().UnixNano())
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

// DeterministicEdgeID generates a stable ID for an edge based on its source, destination, label, and data.
func DeterministicEdgeID(src, dst, label string, data map[string]interface{}) string {
	b, _ := json.Marshal(data)
	return fmt.Sprintf("%x", sha1.Sum([]byte(fmt.Sprintf("%s:%s:%s:%s", src, dst, label, string(b)))))
}

// RandomPort returns a random port string between 10000 and 20000.
func RandomPort() string {
	min := 10000
	max := 40000
	n := rand.Intn(max-min) + min
	return fmt.Sprintf("%d", n)
}
