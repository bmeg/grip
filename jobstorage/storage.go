package jobstorage

import (
  "fmt"
  "encoding/json"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/gripql"
)

type Stream struct {
  Pipe      gdbi.InPipe
  DataType   gdbi.DataType
  MarkTypes map[string]gdbi.DataType
}

type JobStorage interface {
  List(graph string) (chan string, error)
  Spool(graph string, stream *Stream) (string, error)
  Stream(graph, id string) (*Stream, error)
  Delete(graph, id string) error
  Status(graph, id string) (*gripql.JobStatus, error)
}



type FSResults struct {
  BaseDir string
  
}

func (fs *FSResults) List(graph string) (chan string, error) {
  out := make(chan string)
  go func () {
    defer close(out)
  } ()
  return out, nil  
}

func (fs *FSResults) Spool(graph string, stream *Stream) (string, error) {
  id := "test-1"
  go func () {
    for i := range stream.Pipe {
      out, err := json.Marshal(i)
      if err == nil {        
        fmt.Printf("%s\n", out)
      }
    }
  } ()  
  return id, nil
}

func (fs *FSResults) Stream(graph, id string) (*Stream, error) {
  out := make(chan *gdbi.Traveler, 10)
  
  var dt gdbi.DataType
  var markTypes map[string]gdbi.DataType
  
  return &Stream{
    Pipe: out,
    DataType: dt,
    MarkTypes: markTypes,
  }, nil
}

func (fs *FSResults) Delete(graph, id string) error {
  
  return nil
}

func (fs *FSResults) Status(graph, id string) (*gripql.JobStatus, error) {

    return nil, nil
}