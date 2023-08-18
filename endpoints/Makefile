
all: graphql.so cypher.so graphqlv2.so

# requires 'brew install zig' since CGO requires extra tools to crosscompile on Darwin
# -trimpath flag must be used for building extension and the grip object
graphql_gen3_amd64 : $(shell find graphql_gen3 -name "*.go")
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC="zig cc -target x86_64-linux" CXX="zig c++ -target x86_64-linux" go build --tags extended -trimpath --buildmode=plugin ./graphql_gen3 

graphql_gen3 : $(shell find graphql_gen3 -name "*.go")
	go build --buildmode=plugin ./graphql_gen3

graphql.so : $(shell find graphql -name "*.go")
	go build --buildmode=plugin ./graphql

graphqlv2.so : $(shell find graphqlv2 -name "*.go")
	go build --buildmode=plugin ./graphqlv2

cypher.so :  $(shell find cypher -name "*.go")
	go build --buildmode=plugin ./cypher

clean:
	rm *.so