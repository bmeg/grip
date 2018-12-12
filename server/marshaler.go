package server

import (
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

// MarshalClean is a shim class to 'fix' outgoing streamed messages
// in the default implementation, grpc-gateway wraps the individual messages
// of the stream with a {"result" : <value>}. The cleaner idendifies that and
// removes the wrapper
type MarshalClean struct {
	m runtime.Marshaler
}

// ContentType return content type of marshler
func (mclean *MarshalClean) ContentType() string {
	return mclean.m.ContentType()
}

// Marshal serializes v into a JSON encoded byte array. If v is of
// type `proto.Message` the then field "result" is extracted and returned by
// itself. This is mainly to get around a weird behavior of the GRPC gateway
// streaming output
func (mclean *MarshalClean) Marshal(v interface{}) ([]byte, error) {
	if x, ok := v.(map[string]proto.Message); ok {
		if val, ok := x["result"]; ok {
			return mclean.m.Marshal(val)
		}
	}
	return mclean.m.Marshal(v)
}

// NewDecoder shims runtime.Marshaler.NewDecoder
func (mclean *MarshalClean) NewDecoder(r io.Reader) runtime.Decoder {
	return mclean.m.NewDecoder(r)
}

// NewEncoder shims runtime.Marshaler.NewEncoder
func (mclean *MarshalClean) NewEncoder(w io.Writer) runtime.Encoder {
	return mclean.m.NewEncoder(w)
}

// Unmarshal shims runtime.Marshaler.Unmarshal
func (mclean *MarshalClean) Unmarshal(data []byte, v interface{}) error {
	return mclean.m.Unmarshal(data, v)
}
