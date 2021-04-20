package gripql

import (
  "google.golang.org/protobuf/encoding/protojson"
)

func (js *JobStatus) MarshalJSON() ([]byte, error) {
  return protojson.Marshal(js)
}
  
func (js *JobStatus) UnmarshalJSON(data []byte) error {
  return protojson.Unmarshal(data, js)  
}