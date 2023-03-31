
all: graphql.so cypher.so

graphql.so : $(shell find graphql -name "*.go")
	go build --buildmode=plugin ./graphql

cypher.so :  $(shell find cypher -name "*.go")
	go build --buildmode=plugin ./cypher

