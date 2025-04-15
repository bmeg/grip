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

export GRIP_VERSION = 0.7.0
# LAST_PR_NUMBER is used by the release notes builder to generate notes
# based on pull requests (PR) up until the last release.
export LAST_PR_NUMBER = 229

# ---------------------
# Compile and Install
# ---------------------
# Build the code
install:
	@touch version/version.go
	@go install -ldflags '$(VERSION_LDFLAGS)' .

# --------------------------
# Complile Protobuf Schemas
# --------------------------
proto:
	@cd gripql && protoc \
		-I ./ \
		-I ../googleapis \
		--lint_out=. \
		--go_out ./ \
	  	--go_opt paths=source_relative \
		--go-grpc_out ./ \
		--go-grpc_opt paths=source_relative \
		--grpc-gateway_out allow_delete_body=true:./ \
		--grpc-gateway_opt logtostderr=true \
		--grpc-gateway_opt paths=source_relative \
		--grpc-rest-direct_out . \
		gripql.proto
	@cd kvindex && protoc \
		-I ./ \
		--go_opt=paths=source_relative \
		--go_out=. \
		--go_opt paths=source_relative \
		index.proto
	@cd gripper/ && protoc \
	  -I ./ \
		-I ../googleapis/ \
		--go_out . \
		--go_opt paths=source_relative \
		--go-grpc_out ./ \
		--go-grpc_opt paths=source_relative \
		gripper.proto


proto-depends:
	@git submodule update --init --recursive
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@go install github.com/ckaznocha/protoc-gen-lint@latest
	@go install github.com/bmeg/protoc-gen-grpc-rest-direct@latest
	@go install github.com/ckaznocha/protoc-gen-lint@latest

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
	golangci-lint run --disable-all \
		-E gofmt -E goimports -E misspell -E typecheck -E golint -E gosimple -E govet
	flake8 gripql/python/ conformance/

lint-depends:
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1
	go install golang.org/x/tools/cmd/goimports

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
	python conformance/run_conformance.py http://localhost:18201

test-authorization:
	python conformance/run_auth.py http://localhost:18201 $(ARGS)

# ---------------------
# Database development
# ---------------------
start-mongo:
	@docker rm -f grip-mongodb-test > /dev/null 2>&1 || echo
	docker run -d --name grip-mongodb-test -p 27017:27017 mongo:7.0.13-rc0-jammy > /dev/null

start-postgres:
	@docker rm -f grip-postgres-test > /dev/null 2>&1 || echo
	docker run -d --name grip-postgres-test -p 15432:5432 -e POSTGRES_PASSWORD= -e POSTGRES_USER=postgres postgres:10.4 > /dev/null

start-mysql:
	@docker rm -f grip-mysql-test > /dev/null 2>&1 || echo
	docker run -d --name grip-mysql-test -p 13306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.0.11 --default-authentication-plugin=mysql_native_password > /dev/null

start-gripper-test:
	@cd ./gripper/test-graph && ./gripper-table -m swapi/table.map &

start-kafka:
	@docker rm -f kafka > /dev/null 2>&1 || echo
	docker run -d --name kafka \
		-p 9092:9092 \
		-e KAFKA_ENABLE_KRAFT=yes \
		-e KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv== \
		-e KAFKA_CFG_NODE_ID=1 \
		-e KAFKA_CFG_PROCESS_ROLES=controller,broker \
		-e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
		-e KAFKA_CFG_LISTENERS=CONTROLLER://:9093,INTERNAL://:9092 \
		-e KAFKA_CFG_ADVERTISED_LISTENERS=INTERNAL://localhost:9092 \
		-e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT \
		-e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=INTERNAL \
		-e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_CFG_SUPER_USERS=User:admin \
		-e KAFKA_CLIENT_USERS=admin \
		-e KAFKA_CLIENT_PASSWORDS=adminpassword \
		-e KAFKA_CFG_SASL_ENABLED_MECHANISMS=PLAIN \
		-e KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN \
		bitnami/kafka:latest
	sleep 10
	printf '%s\n' \
		'security.protocol=SASL_PLAINTEXT' \
		'sasl.mechanism=PLAIN' \
		'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="adminpassword";' \
		> sasl-config.properties
	docker cp sasl-config.properties kafka:/tmp/sasl-config.properties
	docker exec kafka kafka-topics.sh \
		--create \
		--topic gripHistory \
		--bootstrap-server localhost:9092 \
		--partitions 1 \
		--replication-factor 1 \
		--command-config /tmp/sasl-config.properties
	docker exec kafka kafka-topics.sh \
		--list \
		--bootstrap-server localhost:9092 \
		--command-config /tmp/sasl-config.properties

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

