SHELL=/bin/bash

TESTS=$(shell go list ./... | grep -v /vendor/)

git_commit := $(shell git rev-parse --short HEAD)
git_branch := $(shell git symbolic-ref -q --short HEAD)
git_upstream := $(shell git remote get-url $(shell git config branch.$(shell git symbolic-ref -q --short HEAD).remote) 2> /dev/null)

export GIT_BRANCH = $(git_branch)
export GIT_UPSTREAM = $(git_upstream)
export GO111MODULE=on

VERSION_LDFLAGS=\
 -X "github.com/bmeg/grip/version.BuildDate=$(shell date)" \
 -X "github.com/bmeg/grip/version.GitCommit=$(git_commit)" \
 -X "github.com/bmeg/grip/version.GitBranch=$(git_branch)" \
 -X "github.com/bmeg/grip/version.GitUpstream=$(git_upstream)"

export GRIP_VERSION = 0.5.1
# LAST_PR_NUMBER is used by the release notes builder to generate notes
# based on pull requests (PR) up until the last release.
export LAST_PR_NUMBER = 134

# ---------------------
# Compile and Install
# ---------------------
# Build the code
install:
	@touch version/version.go
	@go install -ldflags '$(VERSION_LDFLAGS)' .

# Build the code including the rocksdb package
with-rocksdb:
	@go install -tags 'rocksdb' -ldflags '$(VERSION_LDFLAGS)' .

local-rocksdb: rocksdb-lib
	@CGO_LDFLAGS="-L$(shell pwd)/rocksdb-lib" CGO_CFLAGS="-I$(shell pwd)/rocksdb-lib/include/" go install -tags 'rocksdb' -ldflags '$(VERSION_LDFLAGS)' .

rocksdb-lib:
	@git clone https://github.com/facebook/rocksdb.git rocksdb-lib
	@pushd rocksdb-lib && make static_lib && popd

bench: rocksdb-lib
	#@CGO_LDFLAGS="-L$(shell pwd)/rocksdb-lib" CGO_CFLAGS="-I$(shell pwd)/rocksdb-lib/include/" go run -tags 'rocksdb' -ldflags '$(VERSION_LDFLAGS)' test/graph-bench/main.go
	go run -tags 'rocksdb' -ldflags '$(VERSION_LDFLAGS)' test/graph-bench/main.go

# --------------------------
# Complile Protobuf Schemas
# --------------------------
proto:
	@cd gripql && protoc \
		-I ./ \
		-I ../googleapis \
		--lint_out=. \
		--go_out=Mgoogle/protobuf/struct.proto=github.com/golang/protobuf/ptypes/struct,plugins=grpc:. \
		--grpc-gateway_out=logtostderr=true:. \
		--grcp-rest-direct_out=. \
		gripql.proto
	@cd kvindex && protoc \
		-I ./ \
		--go_out=. \
		index.proto

proto-depends:
	@git submodule update --init --recursive
	@go get github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	@go get github.com/golang/protobuf/protoc-gen-go
	@go get github.com/ckaznocha/protoc-gen-lint
	@go get github.com/bmeg/protoc-gen-grcp-rest-direct


# ---------------------
# Code Style
# ---------------------
# Automatially update code formatting
tidy:
	@for f in $$(find . -path ./vendor -prune -o -name "*.go" -print | egrep -v "pkg|\.pb\.go|\.gw\.go|\.dgw\.go|underscore\.go"); do \
		gofmt -w -s $$f ;\
		goimports -w $$f ;\
	done;

# Run code style and other checks
lint:
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.18.0
	golangci-lint run --disable-all \
		-E gofmt -E goimports -E misspell -E typecheck -E golint -E gosimple -E govet
	flake8 gripql/python/ conformance/

# ---------------------
# Release / Snapshot
# ---------------------
snapshot:
	@goreleaser release \
		--rm-dist \
		--snapshot

release:
	@goreleaser release \
		--rm-dist \
		--release-notes <(github-release-notes -org bmeg -repo grip -stop-at ${LAST_PR_NUMBER})

release-dep:
	@go get github.com/goreleaser/goreleaser
	@go get github.com/buchanae/github-release-notes


# ---------------------
# Tests
# ---------------------
test:
	@go test $(TESTS)

test-conformance:
	python3 conformance/run_conformance.py http://localhost:18201

# ---------------------
# Database development
# ---------------------
start-mongo:
	@docker rm -f grip-mongodb-test > /dev/null 2>&1 || echo
	docker run -d --name grip-mongodb-test -p 27000:27017 docker.io/mongo:3.6.4 > /dev/null

start-elastic:
	@docker rm -f grip-es-test > /dev/null 2>&1 || echo
	docker run -d --name grip-es-test -p 19200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:5.6.3 > /dev/null

start-postgres:
	@docker rm -f grip-postgres-test > /dev/null 2>&1 || echo
	docker run -d --name grip-postgres-test -p 15432:5432 -e POSTGRES_PASSWORD= -e POSTGRES_USER=postgres postgres:10.4 > /dev/null

start-mysql:
	@docker rm -f grip-mysql-test > /dev/null 2>&1 || echo
	docker run -d --name grip-mysql-test -p 13306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.0.11 --default-authentication-plugin=mysql_native_password > /dev/null

# ---------------------
# Website
# ---------------------
website:
	hugo --source ./website

# Serve the website on localhost:1313
website-dev:
	hugo --source ./website -w server

# ---------------------
# Other
# ---------------------
.PHONY: test rocksdb website
