
all: graphql.so cypher.so graphqlv2.so

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

.PHONY: graphql_gen3 graphql_gen3_amd64