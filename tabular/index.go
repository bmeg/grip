package tabular

import (
  "log"
  "context"
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
  idName string
  idCol int
  idxCols []string
  idxMap map[string]uint64
  header []string
  lineReader *LineReader
  cparse     CSVParse
}

func (t *TabularIndex) IndexTSV(path string, idName string, idxCols []string) *TSVIndex {
  o := TSVIndex{kv:t.kv, path:path, idName:idName, idxCols:idxCols, cparse:CSVParse{}}
  o.Init()
  return &o
}

func (t *TabularIndex) Close() error {
    return t.kv.Close()
}

func (t *TSVIndex) Close() error {
  return t.lineReader.Close()
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

  t.idxMap = map[string]uint64{}
  if _, err := GetLineCount(t.kv, t.pathID); err == nil {
    row := t.cparse.Parse(string(t.lineReader.SeekRead(0)))
    t.header = row
    for i := range row {
      if t.idName == row[i] {
        t.idCol = i
      }
      for j := range t.idxCols {
        if t.idxCols[j] == row[i] {
          t.idxMap[row[i]] = uint64(i)
        }
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
          if t.idName == row[i] {
            t.idCol = i
          }
          for j := range t.idxCols {
            if t.idxCols[j] == row[i] {
              t.idxMap[row[i]] = uint64(i)
            }
          }
        }
      } else {
        SetIDLine(bl, t.pathID, row[t.idCol], count)
        SetLineOffset(bl, t.pathID, count, line.Offset)
        for _, col := range t.idxMap {
          SetColumnIndex(bl, t.pathID, col, row[col], count)
        }
        count++
      }
    }
    SetLineCount(bl, t.pathID, count)
    return nil
  })
  log.Printf("SetupIndexCol: %d", t.idCol)
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

func (t *TSVIndex) GetLineRow(lineNum uint64) (*TableRow, error) {
  text, err := t.GetLineText(lineNum)
  if err != nil {
    return nil, err
  }
  r := t.cparse.Parse(string(text))
  d := map[string]string{}
  for i := 0; i < len(t.header) && i < len(r); i++ {
    if i != t.idCol {
      d[t.header[i]] = r[i]
    }
  }
  o := TableRow{ r[t.idCol], d }
  return &o, nil
}

func (t *TSVIndex) GetRows() chan *TableRow {
  log.Printf("ReadIndexCol: %d", t.idCol)
  out := make(chan *TableRow, 10)
  go func() {
    defer close(out)
    hasHeader := false
    for line := range t.lineReader.ReadLines() {
      if !hasHeader {
        hasHeader = true
      } else {
        r := t.cparse.Parse(string(line.Text))
        d := map[string]string{}
        for i := 0; i < len(t.header) && i < len(r); i++ {
          if i != t.idCol {
            d[t.header[i]] = r[i]
          }
        }
        //log.Printf("Key: %s", r[t.idCol])
        o := TableRow{ r[t.idCol], d }
        out <- &o
      }
    }
  }()
  return out
}

func (t *TSVIndex) GetIDs(ctx context.Context) chan string {
  return GetIDChannel(ctx, t.kv, t.pathID)
}
