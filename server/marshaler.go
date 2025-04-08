package server

import (
	"io"

	"github.com/bmeg/grip/gripql"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MarshalClean is a shim class to 'fix' outgoing streamed messages
// in the default implementation, grpc-gateway wraps the individual messages
// of the stream with a {"result" : <value>}. The cleaner idendifies that and
// removes the wrapper
type MarshalClean struct {
	m runtime.Marshaler
}

func NewMarshaler() runtime.Marshaler {
	return &MarshalClean{
		m: &runtime.JSONPb{
			protojson.MarshalOptions{EmitUnpopulated: true},
			protojson.UnmarshalOptions{},
			//EnumsAsInts:  false,
			//EmitDefaults: true,
			//OrigName:     true,
		},
	}
}

// ContentType return content type of marshler
func (mclean *MarshalClean) ContentType(i interface{}) string {
	return mclean.m.ContentType(i)
}

// Marshal serializes v into a JSON encoded byte array. If v is of
// type `proto.Message` the then field "result" is extracted and returned by
// itself. This is mainly to get around a weird behavior of the GRPC gateway
// streaming output
func (mclean *MarshalClean) Marshal(v interface{}) ([]byte, error) {
	if x, ok := v.(map[string]interface{}); ok {
		if val, ok := x["result"]; ok {
			return mclean.Marshal(val)
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

func FlattenRewriter(_ context.Context, response proto.Message) (interface{}, error) {
	//fmt.Printf("Calling re-writer\n")
	switch v := response.(type) {
	case *gripql.Vertex:
		out := v.Data.AsMap()
		out["_id"] = v.Id
		out["_label"] = v.Label
		return out, nil
	case *gripql.Edge:
		out := v.Data.AsMap()
		out["_id"] = v.Id
		out["_label"] = v.Label
		out["_to"] = v.To
		out["_from"] = v.From
		return out, nil
	case *gripql.QueryResult:
		if e := v.GetVertex(); e != nil {
			out := e.Data.AsMap()
			out["_id"] = e.Id
			out["_label"] = e.Label
			return map[string]any{"vertex": out}, nil
		} else if e := v.GetEdge(); e != nil {
			out := e.Data.AsMap()
			out["_id"] = e.Id
			out["_label"] = e.Label
			out["_to"] = e.To
			out["_from"] = e.From
			return map[string]any{"edge": out}, nil

		}
	}
	return response, nil
}
