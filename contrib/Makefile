
GOPATH := $(shell pwd)
export GOPATH
PATH := ${PATH}:$(shell pwd)/bin
export PATH

build:
	go install github.com/bmeg/arachne/cmd/arachne

PROTO_INC= -I ./ -I ../googleapis

proto:
	cd src/github.com/bmeg/arachne/ophion && protoc \
	$(PROTO_INC) \
	--go_out=\
	Mgoogle/api/annotations.proto=github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,\
	Mgoogle/protobuf/struct.proto=github.com/golang/protobuf/ptypes/struct,\
	Mgoogle/protobuf/any.proto=github.com/golang/protobuf/ptypes/any,\
	plugins=grpc:./ \
	--grpc-gateway_out=logtostderr=true:. \
	ophion.proto

download:
	go get -d github.com/bmeg/arachne
	go get github.com/golang/protobuf/protoc-gen-go
	go get github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway/
	
test:
	go test -v github.com/bmeg/arachne/