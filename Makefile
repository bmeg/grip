ifndef GOPATH
$(error GOPATH is not set)
endif

VERSION = 0.1.0
TESTS=$(shell go list ./... | grep -v /vendor/)
CONFIGDIR=$(shell pwd)/tests

export SHELL=/bin/bash
PATH := ${PATH}:${GOPATH}/bin
export PATH

# Build the code
install: depends
	@go install github.com/bmeg/arachne

# Update submodules and build code
depends:
	@git submodule update --init --recursive
	@go get -d github.com/bmeg/arachne

# Automatially update code formatting
tidy:
	@for f in $$(find ./ -name "*.go" -print | egrep -v "\.pb\.go|\.gw\.go|underscore\.go"); do \
		go fmt $$f ;\
	done;

# Run code style and other checks
lint:
	@go get github.com/alecthomas/gometalinter
	@gometalinter --install > /dev/null
	@gometalinter --disable-all --enable=vet --enable=golint --enable=gofmt --enable=misspell \
		--vendor --errors \
		-e '.*bundle.go' -e ".*pb.go" -e ".*pb.gw.go" -e "underscore.go" \
		./...

# Run code style and other checks
lint-strict:
	@go get github.com/alecthomas/gometalinter
	@gometalinter --install > /dev/null
	@gometalinter --disable-all --enable=vet --enable=golint --enable=gofmt --enable=misspell \
		--vendor \
		-e '.*bundle.go' -e ".*pb.go" -e ".*pb.gw.go"  -e "underscore.go" \
		./...

# Run all tests
test:
	@go test $(TESTS)

# Build binaries for all OS/Architectures
cross-compile: depends
	@echo '=== Cross compiling... ==='
	@for GOOS in darwin linux; do \
		for GOARCH in amd64; do \
			GOOS=$$GOOS GOARCH=$$GOARCH go build -a \
				-ldflags '$(VERSION_LDFLAGS)' \
				-o build/bin/arachne-$$GOOS-$$GOARCH .; \
		done; \
	done

clean-release:
	rm -rf ./build/release

# Upload a release to GitHub
upload-release: clean-release cross-compile
	#
	# NOTE! Making a release requires manual steps.
	# See: website/content/docs/development.md
	@go get github.com/aktau/github-release
	@if [ $$(git rev-parse --abbrev-ref HEAD) != 'master' ]; then \
		echo 'This command should only be run from the master branch'; \
		exit 1; \
	fi
	@if [ -z "$$GITHUB_TOKEN" ]; then \
		echo 'GITHUB_TOKEN is required but not set. Generate one in your GitHub settings at https://github.com/settings/tokens and set it to an environment variable with `export GITHUB_TOKEN=123456...`'; \
		exit 1; \
	fi
	-github-release release \
		-u bmeg \
		-r arachne \
		--tag $(VERSION) \
		--name $(VERSION)
	for f in $$(ls -1 build/bin); do \
		mkdir -p build/release/$$f-$(VERSION); \
		cp build/bin/$$f build/release/$$f-$(VERSION)/arachne; \
		tar -C build/release/$$f-$(VERSION) -czf build/release/$$f-$(VERSION).tar.gz .; \
		github-release upload \
		-u bmeg \
		-r arachne \
		--name $$f-$(VERSION).tar.gz \
		--tag $(VERSION) \
		--replace \
		--file ./build/release/$$f-$(VERSION).tar.gz; \
	done

# Bundle example task messages into Go code.
bundle-examples:
	@go-bindata -pkg examples -o examples/bundle.go $(shell find examples/ -name '*.json')
	@go-bindata -pkg config -o config/bundle.go $(shell find config/ -name '*.txt' -o -name '*.yaml')
	@gofmt -w -s examples/bundle.go config/bundle.go

# Build docker image.
docker: cross-compile
	mkdir -p build/docker
	cp build/bin/arachne-linux-amd64 build/docker/arachne
	cp docker/* build/docker/
	cd build/docker/ && docker build -t arachne .

# Remove build/development files.
clean:
	@rm -rf ./bin ./pkg ./test_tmp ./build ./buildtools
