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

  d := tix.GetLineNumber("24089")
  fmt.Printf("%d\n", d)
  o := tix.GetLineText(d)
  fmt.Printf("%s\n", o)
}
