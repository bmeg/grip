
package gripper

import (
  "net"
  "context"
  "fmt"
  "log"

  "strings"
  "google.golang.org/grpc"
  "google.golang.org/protobuf/types/known/structpb"
)

type BaseRow struct {
	Key   string
	Value map[string]interface{}
}

type Driver interface {
	GetTimeout() int
	GetFields() ([]string, error)
	FetchRow(string) (*BaseRow, error)
	FetchRows(context.Context) (chan *BaseRow, error)
	FetchMatchRows(context.Context, string, string) (chan *BaseRow, error)
}

type SimpleTableServicer struct {
	UnimplementedGRIPSourceServer
	drivers map[string]Driver
}


func NewSimpleTableServer(dr map[string]Driver) *SimpleTableServicer {
	return &SimpleTableServicer{drivers: dr}
}


func StartServer(port int, serv GRIPSourceServer) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	RegisterGRIPSourceServer(grpcServer, serv)
	fmt.Printf("Starting: %d\n", port)
	grpcServer.Serve(lis)
}


func (st *SimpleTableServicer) GetCollections(e *Empty, srv GRIPSource_GetCollectionsServer) error {
	for n := range st.drivers {
		srv.Send(&Collection{Name: n})
	}
	return nil
}

func (st *SimpleTableServicer) GetCollectionInfo(cxt context.Context, col *Collection) (*CollectionInfo, error) {
	if dr, ok := st.drivers[col.Name]; ok {
		o := []string{}
    fields, err := dr.GetFields()
    if err != nil {
      return nil, err
    }
		for _, f := range fields {
			o = append(o, "$." + f)
		}
		return &CollectionInfo{SearchFields: o}, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (st *SimpleTableServicer) GetIDs(col *Collection, srv GRIPSource_GetIDsServer) error {
	if dr, ok := st.drivers[col.Name]; ok {
		ch, _ := dr.FetchRows(srv.Context())
		for row := range ch {
			srv.Send(&RowID{Id: row.Key})
		}
		return nil
	}
	return fmt.Errorf("Not Found")
}

func (st *SimpleTableServicer) GetRows(col *Collection, srv GRIPSource_GetRowsServer) error {
	if dr, ok := st.drivers[col.Name]; ok {
		ch, _ := dr.FetchRows(srv.Context())
		for row := range ch {
			data, _ := structpb.NewStruct(row.Value)
			srv.Send(&Row{Id: row.Key, Data: data})
		}
		return nil
	}
	return fmt.Errorf("Not Found")
}

func (st *SimpleTableServicer) GetRowsByID(srv GRIPSource_GetRowsByIDServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			break
		}
		log.Printf("Request: %s %s", err, req)
		if dr, ok := st.drivers[req.Collection]; ok {
			row, _ := dr.FetchRow(req.Id)
			data, _ := structpb.NewStruct(row.Value)
			srv.Send(&Row{Id: row.Key, Data: data, RequestID: req.RequestID})
		} else {
			//do something here
		}
	}
	return nil
}

func (st *SimpleTableServicer) GetRowsByField(req *FieldRequest, srv GRIPSource_GetRowsByFieldServer) error {
	if dr, ok := st.drivers[req.Collection]; ok {
		field := req.Field
		if strings.HasPrefix(field, "$.") {
			field = field[2:len(field)]
		}
		ch, _ := dr.FetchMatchRows(srv.Context(), field, req.Value)
		for row := range ch {
			data, _ := structpb.NewStruct(row.Value)
			srv.Send(&Row{Id: row.Key, Data: data})
		}
		return nil
	}
	return fmt.Errorf("Not Found")
}
