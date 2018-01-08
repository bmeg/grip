
package graphql

import (
  "net/http"
  "github.com/graphql-go/handler"
  "github.com/graphql-go/graphql/testutil"
)

type GraphQLHandler struct {
  graphqlHadler *handler.Handler
}

func NewHTTPHandler() http.Handler {
  return &GraphQLHandler{handler.New(&handler.Config{
      Schema:   &testutil.StarWarsSchema,
    })}
}


func (self *GraphQLHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
  self.graphqlHadler.ServeHTTP(writer, request)
}
