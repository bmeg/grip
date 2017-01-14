package gdbi

import (
  "github.com/bmeg/arachne/ophion"
)

type QueryInterface interface {
  V(key ... string) QueryInterface
  E() QueryInterface
  Count() QueryInterface
  Has(prop string, value string) QueryInterface
  Out(key ... string) QueryInterface
  In(key ... string) QueryInterface
  
  //Read write methods
  AddV(key string) QueryInterface
  AddE(key string) QueryInterface
  To(key string) QueryInterface
  Property(key string, value string) QueryInterface
  
  Execute() chan ophion.QueryResult
  Run() error //Do execute, but throw away the results
}

type ArachneInterface interface {
  Close()
  Query() QueryInterface
}