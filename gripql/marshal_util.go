package gripql

import (
	"google.golang.org/protobuf/encoding/protojson"
)

// These are some extra MarshalJSON/UnmarshalJSON to make sure that
// some of the Protobuf structures get handled when that are passed
// to the golang encoding/json package, rather then the protojson

func (js *JobStatus) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(js)
}

func (js *JobStatus) UnmarshalJSON(data []byte) error {
	return protojson.Unmarshal(data, js)
}
