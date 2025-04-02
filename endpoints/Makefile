
all: graphql.so cypher.so graphqlv2.so

graphql.so : $(shell find graphql -name "*.go")
	go build --buildmode=plugin ./graphql

graphqlv2.so : $(shell find graphqlv2 -name "*.go")
	go build --buildmode=plugin ./graphqlv2

cypher.so :  $(shell find cypher -name "*.go")
	go build --buildmode=plugin ./cypher

clean:
	rm *.so