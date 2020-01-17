package server

import (
	"strings"
	"time"

	"github.com/bmeg/grip/log"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func extractHeaderKeys(input map[string][]string, whitelist []string) map[string][]string {
	filtered := make(map[string][]string)
	if input == nil {
		return filtered
	}
	for k, v := range input {
		for _, w := range whitelist {
			if strings.ToLower(k) == strings.ToLower(w) {
				filtered[strings.ToLower(k)] = v
				break
			}
		}
	}
	return filtered
}

// Return a new interceptor function that logs all requests at the Info level
func unaryInterceptor(enabled bool, whitelist []string) grpc.UnaryServerInterceptor {
	// Return a function that is the interceptor.
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !enabled {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		md, _ := metadata.FromIncomingContext(ctx)
		headers := extractHeaderKeys(md, whitelist)
		entry := log.WithFields(log.Fields{
			"path":    info.FullMethod,
			"request": req,
			"headers": headers,
			"latency": time.Since(start).String(),
			"status":  runtime.HTTPStatusFromCode(status.Code(err)),
		})
		if err == nil {
			entry.Info("gRPC server responded")
		} else {
			entry.WithField("error", err).Error("gRPC server responded")
		}
		return resp, err
	}
}

// Return a new interceptor function that logs all requests at the Info level
// https://github.com/grpc-ecosystem/go-grpc-middleware/blob/6f8030a0b4ee588a3f33556266b552a90a5574e2/logging/logrus/payload_interceptors.go#L46
func streamInterceptor(enabled bool, whitelist []string) grpc.StreamServerInterceptor {
	// Return a function that is the interceptor.
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !enabled {
			return handler(srv, ss)
		}
		start := time.Now()
		md, _ := metadata.FromIncomingContext(ss.Context())
		headers := extractHeaderKeys(md, whitelist)
		newStream := &loggingServerStream{ServerStream: ss}
		err := handler(srv, newStream)
		entry := log.WithFields(log.Fields{
			"path":    info.FullMethod,
			"request": newStream.request,
			"headers": headers,
			"latency": time.Since(start).String(),
			"status":  runtime.HTTPStatusFromCode(status.Code(err)),
		})
		if err == nil {
			entry.Info("gRPC server responded")
		} else {
			entry.WithField("error", err).Error("gRPC server responded")
		}
		return err
	}
}

type loggingServerStream struct {
	grpc.ServerStream
	request interface{}
}

func (l *loggingServerStream) SendMsg(m interface{}) error {
	return l.ServerStream.SendMsg(m)
}

func (l *loggingServerStream) RecvMsg(m interface{}) error {
	l.request = m
	return l.ServerStream.RecvMsg(m)
}
