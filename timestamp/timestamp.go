package timestamp

import (
	"fmt"
	"time"
)

//Timestamp records timestamps
type Timestamp struct {
	stamps map[string]string
}

//NewTimestamp creates a new Timestamp recorder
func NewTimestamp() Timestamp {
	return Timestamp{stamps: map[string]string{}}
}

//Touch updates an entry in the timestamp
func (ts *Timestamp) Touch(name string) {
	ts.stamps[name] = fmt.Sprintf("%d", time.Now().UnixNano())
}

//Get gets the current timestamp
func (ts *Timestamp) Get(name string) string {
	return ts.stamps[name]
}
