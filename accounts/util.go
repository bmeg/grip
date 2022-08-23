package accounts

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/grpc"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc/metadata"
)

func (c *Config) init() {
	if c.auth != nil && c.access != nil {
		return
	}

	if c.Auth != nil {
		if c.Auth.Basic != nil {
			c.auth = c.Auth.Basic
		}
	}
	if c.auth == nil {
		c.auth = NullAuth{}
	}
	if c.Access != nil {
		if c.Access.Casbin != nil {
			c.access = c.Access.Casbin
		}
	}
	if c.access == nil {
		c.access = NullAccess{}
	}
}

func (c *Config) UnaryInterceptor() grpc.UnaryServerInterceptor {
	c.init()
	return unaryAuthInterceptor(c.auth, c.access)
}

func (c *Config) StreamInterceptor() grpc.StreamServerInterceptor {
	c.init()
	return streamAuthInterceptor(c.auth, c.access)
}

// Return a new interceptor function that authorizes RPCs
// using a password stored in the config.
func unaryAuthInterceptor(auth Authenticate, access Access) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		//fmt.Printf("AuthInt: %#v\n", ctx)
		md, _ := metadata.FromIncomingContext(ctx)
		//fmt.Printf("Metadata: %#v\n", md)
		//omd, _ := metadata.FromOutgoingContext(ctx)
		//fmt.Printf("Raw: %#v\n", omd)

		metaData := MetaData{}
		for i := range md {
			metaData[i] = md[i]
		}

		user, err := auth.Validate(metaData)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "PermissionDenied")
		}

		if op, ok := MethodMap[info.FullMethod]; ok {
			graph, err := getUnaryRequestGraph(req, info)
			if err != nil {
				return nil, status.Error(codes.Unknown, fmt.Sprintf("Unable to get graph: %s", err))
			}
			err = access.Enforce(user, graph, op)
			if err != nil {
				return nil, status.Error(codes.PermissionDenied, "PermissionDenied")
			}
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unknown, "Unknown method")
	}
}

// Return a new interceptor function that authorizes RPCs
// using a password stored in the config.
func streamAuthInterceptor(auth Authenticate, access Access) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		//fmt.Printf("Streaming query: %#v\n", info)
		md, _ := metadata.FromIncomingContext(ss.Context())

		//fmt.Printf("Metadata: %#v\n", md)
		metaData := MetaData{}
		for i := range md {
			metaData[i] = md[i]
		}

		user, err := auth.Validate(metaData)
		if err != nil {
			return status.Error(codes.Unauthenticated, "PermissionDenied")
		}

		//current GripQL schema does not support bi-directional streaming
		//mainly because it can't be offered via HTTP based interface
		if info.IsServerStream {
			//ssWrapper := ServerStreamWrapper{ss}

			switch info.FullMethod {
			case "/gripql.Query/Traversal":
				w, err := NewStreamOutWrapper[gripql.GraphQuery](ss)
				if err != nil {
					return status.Error(codes.Unknown, "Request error")
				}
				err = access.Enforce(user, w.Request.Graph, Read)
				if err != nil {
					return status.Error(codes.PermissionDenied, "PermissionDenied")
				}
				return handler(srv, w)
			case "/gripql.Job/ListJobs":
				//TODO: filter list of jobs
				return handler(srv, ss)
			case "/gripql.Job/ResumeJob":
				//TODO: filter list of jobs
				return handler(srv, ss)
			case "/gripql.Job/ViewJob":
				//TODO: filter list of jobs
				return handler(srv, ss)
			case "/gripql.Job/SearchJobs":
				//TODO: filter list of jobs
				return handler(srv, ss)
			}
			log.Errorf("Unknown streaming output: %#v", info)
			return handler(srv, ss)
		} else if info.IsClientStream {
			if info.FullMethod == "/gripql.Edit/BulkAdd" {
				//This checks permission on a per entity basis
				//unfortunatly because of limitations in Protobuf input
				//stream URL formatting, each write request can
				//reference a different graph
				return handler(srv, &BulkWriteFilter{ss, user, access})
			} else {
				log.Errorf("Unknown input streaming op %#v!!!", info)
				return handler(srv, ss)
			}
		}

		return status.Error(codes.Unknown, "Unknown method")
	}
}

func getUnaryRequestGraph(req interface{}, info *grpc.UnaryServerInfo) (string, error) {
	switch info.FullMethod {
	case "/gripql.Query/Traversal", "/gripql.Job/Submit",
		"/gripql.Job/SearchJobs":
		o := req.(*gripql.GraphQuery)
		return o.Graph, nil
	case "/gripql.Query/GetVertex", "/gripql.Query/GetEdge":
		o := req.(*gripql.ElementID)
		return o.Graph, nil
	case "/gripql.Query/GetTimestamp", "/gripql.Query/GetSchema",
		"/gripql.Query/GetMapping", "/gripql.Query/ListIndices",
		"/gripql.Query/ListLabels", "/gripql.Job/ListJobs",
		"/gripql.Edit/AddGraph", "/gripql.Edit/DeleteGraph":
		o := req.(*gripql.GraphID)
		return o.Graph, nil
	case "/gripql.Query/ListGraphs", "/gripql.Query/ListTables":
		return "*", nil
	case "/gripql.Job/GetJob", "/gripql.Job/DeleteJob",
		"/gripql.Job/ViewJob":
		o := req.(*gripql.QueryJob)
		return o.Graph, nil
	case "/gripql.Job/ResumeJob":
		o := req.(*gripql.ExtendQuery)
		return o.Graph, nil
	case "/gripql.Edit/AddVertex", "/gripql.Edit/AddEdge":
		o := req.(*gripql.GraphElement)
		return o.Graph, nil
	case "/gripql.Edit/DeleteVertex", "/gripql.Edit/DeleteEdge":
		o := req.(*gripql.ElementID)
		return o.Graph, nil
	case "/gripql.Edit/AddIndex", "/gripql.Edit/DeleteIndex":
		o := req.(*gripql.IndexID)
		return o.Graph, nil
	case "/gripql.Edit/AddSchema", "/gripql.Edit/AddMapping":
		o := req.(*gripql.Graph)
		return o.Graph, nil
	case "/gripql.Edit/SampleSchema":
		o := req.(*gripql.GraphID)
		return o.Graph, nil
	case "/gripql.Configure/StartPlugin", "/gripql.Configure/ListPlugins", "/gripql.Configure/ListDrivers":
		return "*", nil //these operations effect all graphs
	}

	return "", fmt.Errorf("unknown op: %s", info.FullMethod)
}
