ifndef GOPATH
$(error GOPATH is not set)
endif

VERSION = 0.1.0
TESTS=$(shell go list ./... | grep -v /vendor/)
CONFIGDIR=$(shell pwd)/tests

export SHELL=/bin/bash
PATH := ${PATH}:${GOPATH}/bin
export PATH

# ---------------------
# Compile and Install
# ---------------------
# Build the code
install: depends
	@go install github.com/bmeg/arachne

# Update submodules and build code
depends:
	@git submodule update --init --recursive
	@go get -d github.com/bmeg/arachne

# --------------------------
# Complile Protobuf Schemas
# --------------------------
proto:
	@go get github.com/ckaznocha/protoc-gen-lint
	cd aql && protoc \
	-I ./ -I ../googleapis \
	--lint_out=. \
	--go_out=\
	Mgoogle/protobuf/struct.proto=github.com/golang/protobuf/ptypes/struct,\
	plugins=grpc:./ \
	--grpc-gateway_out=logtostderr=true:. \
	aql.proto

kvproto:
	cd kvindex && protoc \
	-I ./ --go_out=. \
	index.proto

proto-depends:
	go install github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	go install github.com/golang/protobuf/protoc-gen-go

# ---------------------
# Code Style
# ---------------------
# Automatially update code formatting
tidy:
	@for f in $$(find . -name "*.go" -print | egrep -v "\.pb\.go|\.gw\.go|underscore\.go"); do \
		gofmt -w -s $$f ;\
		goimports -w $$f ;\
	done;

# Run code style and other checks
lint:
	@go get github.com/alecthomas/gometalinter
	@gometalinter --install > /dev/null
	@gometalinter --disable-all --enable=vet --enable=golint --enable=gofmt --enable=misspell \
		--vendor \
		-e '.*bundle.go' -e ".*pb.go" -e ".*pb.gw.go"  -e "underscore.go" \
		./...

# ---------------------
# Tests
# ---------------------
test:
	@go test $(TESTS)

start-test-server:
	arachne server --rpc 18202 --port 18201 &

start-test-mongo-server:
	arachne server --rpc 18202 --port 18201 --mongo localhost:2700 &

start-test-elastic-server:
	arachne server --rpc 18202 --port 18201 --elastic localhost:9200 &

test-conformance:
	python conformance/run_conformance.py http://localhost:18201

# ---------------------
# Database development
# ---------------------
start-mongodb:
	@docker rm -f arachne-mongodb-test > /dev/null 2>&1 || echo
	@docker run -d --name arachne-mongodb-test -p 27000:27017 docker.io/mongo:3.5.13 > /dev/null

start-elasticsearch:
	@docker rm -f arachne-es-test > /dev/null 2>&1 || echo
	@docker run -d --name arachne-es-test -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:5.6.3 > /dev/null
