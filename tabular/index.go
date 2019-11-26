package tabular

import (
  "log"
  "github.com/bmeg/grip/kvi"
  "github.com/bmeg/grip/kvi/boltdb"
)

type TabularIndex struct {
  kv kvi.KVInterface
}

func NewTablularIndex(path string) (*TabularIndex, error) {
  out := TabularIndex{}
  kv, err := boltdb.NewKVInterface(path, kvi.Options{})
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
}

func (t *TabularIndex) IndexTSV(path string, indexName string) *TSVIndex {
  o := TSVIndex{kv:t.kv, path:path, indexName:indexName}
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

  count := uint64(0)
  t.kv.BulkWrite(func(bl kvi.KVBulkWrite) error{
    SetPathValue(bl, t.path, t.pathID)
    cparse := CSVParse{}
    for line := range t.lineReader.ReadLines() {
      row := cparse.Parse(string(line.Text))
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
    return nil
  })

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
