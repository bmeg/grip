
package main


import (
  "os"
  "io"
  "flag"
  "fmt"
  "github.com/knakk/rdf"
  "compress/gzip"
)


func main() {
	flag.Parse()

  f, err := os.Open(flag.Arg(0))
  if err != nil {
      // handle error
  }
  fz, _ := gzip.NewReader(f)

  dec := rdf.NewTripleDecoder(fz, rdf.RDFXML)
  for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
    fmt.Printf("%s\n", triple)
  }
}
