package timestamp

import (
	"fmt"
	"sync"
	"time"
)

//Timestamp records timestamps
type Timestamp struct {
	stamps sync.Map
}

//NewTimestamp creates a new Timestamp recorder
func NewTimestamp() Timestamp {
	return Timestamp{stamps: sync.Map{}}
}

//Touch updates an entry in the timestamp
func (ts *Timestamp) Touch(name string) {
	ts.stamps.Store(name, fmt.Sprintf("%d", time.Now().UnixNano()))
}

//Get gets the current timestamp
func (ts *Timestamp) Get(name string) string {
	o, _ := ts.stamps.Load(name)
	if o == nil {
		return ""
	}
	return o.(string)
}
