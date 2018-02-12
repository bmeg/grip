package engine

import (
  "context"
  "github.com/bmeg/arachne/aql"
  "github.com/bmeg/arachne/protoutil"
)

type processor interface {
  process(in reader, out writer)
}

type lookupVerts struct {
  db DB
  ids []string
  labels []string
}

func (l *lookupVerts) process(in reader, out writer) {
  for range in {
    for v := range db.GetVertexList(context.Background(), false) {
      // TODO maybe don't bother copying the data
      out <- &traveler{
        id: v.Gid,
        label: v.Label,
        data: protoutil.AsMap(v.Data),
        dataType: vertexData,
      }
    }
  }
}

type lookupEdges struct {
  db DB
  ids []string
  labels []string
}

func (l *lookupEdges) process(in reader, out writer) {
  for range in {
    for v := range db.GetEdgeList(context.Background(), false) {
      out <- &traveler{
        id: v.Gid,
        label: v.Label,
        data: protoutil.AsMap(v.Data),
        dataType: edgeData,
      }
    }
  }
}

type lookupAdj struct {
  db DB
  dir direction
  labels []string
}

func (l *lookupAdj) process(in reader, out writer) {
}

type lookupEnd struct {
  db DB
  dir direction
  labels []string
}

func (l *lookupEnd) process(in reader, out writer) {
}

type hasData struct {
  stmt *aql.HasStatement
}

func (h *hasData) process(in reader, out writer) {
  for t := range in {
    if t.data == nil {
      continue
    }
		if z, ok := t.data[h.stmt.Key]; ok {
      if s, ok := z.(string); ok && contains(h.stmt.Within, s) {
        out <- t
      }
    }
  }
}

type hasLabel struct {
  labels []string
}
func (h *hasLabel) process(in reader, out writer) {
  for t := range in {
    if contains(h.labels, t.label) {
      out <- t
    }
  }
}

type hasID struct {
  ids []string
}
func (h *hasID) process(in reader, out writer) {
  for t := range in {
    if contains(h.ids, t.id) {
      out <- t
    }
  }
}

type count struct {}
func (c *count) process(in reader, out writer) {
  var i int64
  for range in {
    i++
  }
  out <- &traveler{
    dataType: countData,
    count: i,
  }
}

type limit struct {
  count int64
}
func (l *limit) process(in reader, out writer) {
  var i int64
  for t := range in {
    if i == l.count {
      return
    }
    out <- t
    i++
  }
}

type groupCount struct {
  key string
}

// TODO except, if you select.by("name") this is counting by value, not ID
func (g *groupCount) countIDs(in reader, counts map[string]int64) {
  for t := range in {
    counts[t.id]++
  }
}

func (g *groupCount) countValues(in reader, counts map[string]int64) {
  for t := range in {
    if t.data == nil {
      continue
    }
    if vi, ok := t.data[g.key]; ok {
      // TODO only counting string values.
      //      how to handle other simple types? (int, etc)
      //      what to do for objects? gremlin returns an error.
      //      how to return errors? Add Error travelerType?
      if s, ok := vi.(string); ok {
        counts[s]++
      }
    }
  }
}

func (g *groupCount) process(in reader, out writer) {
  counts := map[string]int64{}

  if g.key != "" {
    g.countValues(in, counts)
  } else {
    g.countIDs(in, counts)
  }

  eo := &traveler{
    dataType: groupCountData,
    groupCounts: counts,
  }
  out <- eo
}


type marker struct {
  marks []string
}

func (m *marker) process(in reader, out writer) {
  for t := range in {
    // Processors are not synchronized; they are independent, concurrent, and buffered.
    // Marks must be copied when written, so that a downstream processor is guaranteed
    // a consistent view of the marks.
    marks := t.marks
    t.marks = map[string]*traveler{}
    // copy the existing marks
    for k, v := range marks {
      t.marks[k] = v
    }
    // add the new marks
    for _, k := range m.marks {
      t.marks[k] = t
    }
    out <- t
  }
}

type selectOne struct {
  mark string
}

func (s *selectOne) process(in reader, out writer) {
  for t := range in {
    x := t.marks[s.mark]
    out <- x
  }
}

type selectMany struct {
  marks []string
}

func (s *selectMany) process(in reader, out writer) {
  for t := range in {
    row := make([]*traveler, len(s.marks))
    for _, mark := range s.marks {
      // TODO handle missing mark? rely on compiler to check this?
      row = append(row, t.marks[mark])
    }
    out <- &traveler{
      dataType: rowData,
      row: row,
    }
  }
}
