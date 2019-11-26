package main


import (
  "os"
  "fmt"
  "github.com/bmeg/grip/tabular"
)


func main() {
  file := os.Args[1]
  indexCol := os.Args[2]
  idx, _ := tabular.NewTablularIndex("table.db")
  tix := idx.IndexTSV(file, indexCol)
  fmt.Printf("Index: %#v\n", tix)

  if d, err := tix.GetLineNumber("13"); err == nil {
    fmt.Printf("%d\n", d)
    if o, err := tix.GetLineText(d); err == nil {
      fmt.Printf("%s\n", o)
    } else {
      fmt.Printf("Error: %s\n", err)
    }
  } else {
    fmt.Printf("Error: %s\n", err)
  }
}
