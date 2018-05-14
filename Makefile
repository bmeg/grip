ifndef GOPATH
$(error GOPATH is not set)
endif

VERSION = 0.2.0
TESTS=$(shell go list ./... | grep -v /vendor/)

export SHELL=/bin/bash
PATH := ${PATH}:${GOPATH}/bin
export PATH

# ---------------------
# Compile and Install
# ---------------------
# Build the code
install: depends
	@go install .

# Update submodules and build code
depends:
	@git submodule update --init --recursive
	@go get github.com/golang/dep/cmd/dep
	@dep ensure

# Build the code including the rocksdb package
with-rocksdb: depends
	@go install -tags 'rocksdb' .

# --------------------------
# Complile Protobuf Schemas
# --------------------------
proto:
	@go get github.com/ckaznocha/protoc-gen-lint
	@cd aql && protoc \
		-I ./ \
		-I ../googleapis \
		--lint_out=. \
		--go_out=Mgoogle/protobuf/struct.proto=github.com/golang/protobuf/ptypes/struct,plugins=grpc:. \
		--grpc-gateway_out=logtostderr=true:. \
		aql.proto
	@cd kvindex && protoc \
		-I ./ \
		--go_out=. \
		index.proto

proto-depends:
	@go get github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	@go get github.com/golang/protobuf/protoc-gen-go
	@go get github.com/ckaznocha/protoc-gen-lint

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
		-e '.*bundle.go' -e ".*pb.go" -e ".*pb.gw.go" -e "underscore.go" \
		./...

# ---------------------
# Tests
# ---------------------
test:
	@go test $(TESTS)

test-conformance:
	python conformance/run_conformance.py http://localhost:18201

start-test-badger-server:
	arachne server --rpc-port 18202 --http-port 18201 --database badger

start-test-mongo-server:
	arachne server --rpc 18202 --port 18201 --database mongo --mongo-url localhost:27000

start-test-elastic-server:
	arachne server --rpc 18202 --port 18201 --database elastic --elastic-url http://localhost:9200

# ---------------------
# Database development
# ---------------------
start-mongo:
	@docker rm -f arachne-mongodb-test > /dev/null 2>&1 || echo
	docker run -d --name arachne-mongodb-test -p 27000:27017 docker.io/mongo:3.5.13 > /dev/null

start-elastic:
	@docker rm -f arachne-es-test > /dev/null 2>&1 || echo
	docker run -d --name arachne-es-test -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:5.6.3 > /dev/null

# ---------------------
# Other
# ---------------------
.PHONY: test rocksdb
