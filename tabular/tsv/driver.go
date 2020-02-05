package tsv

import (
  "log"
  "context"
  "github.com/bmeg/grip/tabular"
)


type TSVDriver struct {
  man  *tabular.TableManager
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

func TSVDriverBuilder(url string, manager *tabular.TableManager, opts tabular.Options) (tabular.Driver, error) {
  o := TSVDriver{path:url, idName:opts.PrimaryKey, idxCols:opts.IndexedColumns, man:manager}
  if err := o.Init(); err != nil {
    return nil, err
  }
  return &o, nil
}

var loaded = tabular.AddDriver("tsv", TSVDriverBuilder)

func (t *TSVDriver) Close() error {
  return t.lineReader.Close()
}

func (t *TSVDriver) Init() error {

  hasHeader := false
  var err error
  t.lineReader, err = NewLineReader(t.path)
  if err != nil {
    return err
  }

  if i, err := t.man.Index.GetPathID(t.path); err == nil {
    t.pathID = i
  } else {
    t.pathID = t.man.Index.NewPathID(t.path)
    //SetPathValue(bl, t.path, t.pathID)
  }

  t.idxMap = map[string]uint64{}
  if _, err := t.man.Index.GetLineCount(t.pathID); err == nil {
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
  t.man.Index.IndexWrite(func(bl *tabular.IndexWriter) error{
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
        bl.SetIDLine(t.pathID, row[t.idCol], count)
        bl.SetLineOffset(t.pathID, count, line.Offset)
        for _, col := range t.idxMap {
          bl.SetColumnIndex(t.pathID, col, row[col], count)
        }
        count++
      }
    }
    bl.SetLineCount(t.pathID, count)
    return nil
  })
  log.Printf("SetupIndexCol: %d [%#v]", t.idCol, t.idxCols)
  log.Printf("Found %d rows", count)
  return nil
}


func (t *TSVDriver) GetLineNumber(id string) (uint64, error) {
  return t.man.Index.GetIDLine(t.pathID, id)
}

func (t *TSVDriver) GetRowByID(id string) (*tabular.TableRow, error) {
  ln, err := t.GetLineNumber(id)
  if err != nil {
    return nil, err
  }
  return t.GetLineRow(ln)
}


func (t *TSVDriver) GetLineText(lineNum uint64) ([]byte, error) {
  offset, err := t.man.Index.GetLineOffset(t.pathID, lineNum)
  if err != nil {
    return nil, err
  }
  //cparse := CSVParse{}
  //log.Printf("LineOffset: %d", offset)
  return t.lineReader.SeekRead(offset), nil
}

func (t *TSVDriver) GetLineRow(lineNum uint64) (*tabular.TableRow, error) {
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
  o := tabular.TableRow{ r[t.idCol], d }
  return &o, nil
}

func (t *TSVDriver) GetRows(ctx context.Context) chan *tabular.TableRow {
  log.Printf("ReadIndexCol: %d", t.idCol)
  out := make(chan *tabular.TableRow, 10)
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
        o := tabular.TableRow{ r[t.idCol], d }
        out <- &o
      }
    }
  }()
  return out
}

func (t *TSVDriver) GetIDs(ctx context.Context) chan string {
  return t.man.Index.GetIDChannel(ctx, t.pathID)
}
