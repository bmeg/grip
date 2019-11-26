package tabular

import (
  "log"
  "github.com/bmeg/grip/kvi"
  //"github.com/bmeg/grip/kvi/boltdb"
  "github.com/bmeg/grip/kvi/badgerdb"
)

type TabularIndex struct {
  kv kvi.KVInterface
}

type TableRow struct {
  Key    string
  Values map[string]string
}

func NewTablularIndex(path string) (*TabularIndex, error) {
  out := TabularIndex{}
  //kv, err := boltdb.NewKVInterface(path, kvi.Options{})
  kv, err := badgerdb.NewKVInterface(path, kvi.Options{})
  if err != nil {
    return nil, err
  }
  out.kv = kv
  return &out, nil
}

type TSVIndex struct {
  kv kvi.KVInterface
  path string
  pathID uint64
  indexName string
  indexCol int
  header []string
  lineReader *LineReader
  cparse     CSVParse
}

func (t *TabularIndex) IndexTSV(path string, indexName string) *TSVIndex {
  o := TSVIndex{kv:t.kv, path:path, indexName:indexName, cparse: CSVParse{}}
  o.Init()
  return &o
}

func (t *TSVIndex) Init() error {

  hasHeader := false
  var err error
  t.lineReader, err = NewLineReader(t.path)
  if err != nil {
    return err
  }

  if i, err := GetPathID(t.kv, t.path); err == nil {
    t.pathID = i
  } else {
    t.pathID = NewPathID(t.kv, t.path)
    //SetPathValue(bl, t.path, t.pathID)
  }

  if _, err := GetLineCount(t.kv, t.pathID); err == nil {
    row := t.cparse.Parse(string(t.lineReader.SeekRead(0)))
    t.header = row
    for i := range row {
      if t.indexName == row[i] {
        t.indexCol = i
      }
    }
    //file have already been indexed
    return nil
  }

  count := uint64(0)
  t.kv.BulkWrite(func(bl kvi.KVBulkWrite) error{
    for line := range t.lineReader.ReadLines() {
      row := t.cparse.Parse(string(line.Text))
      if !hasHeader {
        t.header = row
        hasHeader = true
        for i := range row {
          if t.indexName == row[i] {
            t.indexCol = i
          }
        }
      } else {
        SetIDLine(bl, t.pathID, row[t.indexCol], count)
        SetLineOffset(bl, t.pathID, count, line.Offset)
        count++
      }
    }
    SetLineCount(bl, t.pathID, count)
    return nil
  })
  log.Printf("SetupIndexCol: %s", t.indexCol)
  log.Printf("Found %d rows", count)
  return nil
}


func (t *TSVIndex) GetLineNumber(id string) (uint64, error) {
  return GetIDLine(t.kv, t.pathID, id)
}

func (t *TSVIndex) GetLineText(lineNum uint64) ([]byte, error) {
  offset, err := GetLineOffset(t.kv, t.pathID, lineNum)
  if err != nil {
    return nil, err
  }
  //cparse := CSVParse{}
  log.Printf("LineOffset: %d", offset)
  return t.lineReader.SeekRead(offset), nil
}

func (t *TSVIndex) GetRows() chan *TableRow {
  log.Printf("ReadIndexCol: %s", t.indexCol)
  out := make(chan *TableRow, 10)
  go func() {
    defer close(out)
    for line := range t.lineReader.ReadLines() {
      r := t.cparse.Parse(string(line.Text))
      d := map[string]string{}
      for i := range t.header {
        if i != t.indexCol {
          d[t.header[i]] = r[i]
        }
      }
      //log.Printf("Key: %s", r[t.indexCol])
      o := TableRow{ r[t.indexCol], d }
      out <- &o
    }
  }()
  return out
}
