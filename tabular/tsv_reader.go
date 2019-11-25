package tabular

import (
  "os"
  "bufio"
  "strings"
)

type Line struct {
  Offset uint64
  Text   []byte
}


type LineReader struct {
  file *os.File
}


func NewLineReader(path string) (*LineReader, error) {
  if file, err := os.Open(path); err != nil {
    return nil, err
  } else {
    return &LineReader{file}, nil
  }
}


func (l *LineReader) Close() {
  l.file.Close()
}


func (l *LineReader) ReadLines() (chan Line) {
  l.file.Seek(0, os.SEEK_SET)
  var offset uint64 = 0
  var lastOffset uint64 = 0
  out := make(chan Line, 100)
  go func() {
    reader := bufio.NewReaderSize(l.file, 102400)
    var isPrefix bool = true
    var err error = nil
    var line, ln []byte
    for err == nil {
      line, isPrefix, err = reader.ReadLine()
      ln = append(ln, line...)
      offset += uint64(len(line))
      if !isPrefix {
        if len(ln) > 0 {
          out <- Line{lastOffset,ln}
          lastOffset = offset
          ln = []byte{}
        }
      }
    }
    close(out)
 } ()
 return out
}


type CSVParse struct {
  Comma string
  Comment string
}

func (c *CSVParse) Parse(line string) []string {
  comma := c.Comma
  if comma == "" {
    comma = ","
  }
  comment := c.Comment
  if comment != "" {
    line = strings.Split(line, comment)[0]
  }
  return strings.Split(line, comma)
}
