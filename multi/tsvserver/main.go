package main

import (
	"fmt"
  "io/ioutil"
	"context"
  "strings"
	flag "github.com/spf13/pflag"
	"log"
	"net/http"
  "path/filepath"

	"encoding/json"

	"github.com/bmeg/grip/multi"
  "github.com/bmeg/grip/multi/tsv"
	"github.com/bmeg/grip/multi/kvcache"

	"github.com/gorilla/mux"
)

func homeLink(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Test TSV Server")
}

var driverMap map[string]multi.Driver


func TableListHandler(w http.ResponseWriter, r *http.Request) {
	out := []string{}
	for k := range driverMap {
		out = append(out, k)
	}
	j, _ := json.Marshal(out)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s\n", string(j))
}


func KeyListHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	table := vars["table"]
	d, ok := driverMap[table]
	if !ok {
		//TODO: write error message
		return
	}

	out := []string{}
	for i := range d.GetIDs(context.Background()) {
		out = append(out, i)
	}
	j, _ := json.Marshal(out)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s\n", string(j))

}

func RowHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	table := vars["table"]
	id := vars["id"]

	d, ok := driverMap[table]
	if !ok {
		//TODO: write error message
		return
	}

	row, err := d.GetRowByID(id)
	if err != nil {
		//TODO: write error message
		return
	}
	j, _ := json.Marshal( map[string]interface{}{ "id" : row.Key, "data" : row.Values} )
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s\n", string(j))
}


func main() {
	var port *int = flag.Int("port", 8080, "Port number")
	var cachePath *string = flag.String("cache", "tsv-cache.db", "Cache of TSV offsets")
	var mapPath *string = flag.String("tables", "tables.map", "Map of tables names to TSVs")


	flag.Parse()

  a, _ := filepath.Abs(*mapPath)
  mapPath = &a

	mapTxt, err := ioutil.ReadFile(*mapPath)
  if err != nil {
		log.Printf("Error: %s", err)
		return
	}

	cache, err := kvcache.KVCacheBuilder(*cachePath)
	if err != nil {
		log.Printf("Error: %s", err)
		return
	}

  driverMap = make(map[string]multi.Driver)

  lines := strings.Split(string(mapTxt), "\n")
  for _, line := range lines {
    row := strings.Split(line, "\t")
    if len(row) == 2 {
      name := row[0]
      path := row[1]
      path = filepath.Join(filepath.Dir(*mapPath), path)
      fmt.Printf("%s %s\n", name, path)

			opts := multi.Options{PrimaryKey:"id", Config:map[string]interface{}{ "delim" : "\t" }}
			dr, err := tsv.TSVDriverBuilder(name, path, cache, opts)
			if err == nil {
				driverMap[name] = dr
			} else {
				log.Printf("Error loading Driver: %s", err)
			}
    }
  }

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", homeLink)
	router.HandleFunc("/api/", TableListHandler)
	router.HandleFunc("/api/{table}", KeyListHandler).Name("RowHandler")
	router.HandleFunc("/api/{table}/{id}", RowHandler).Name("RowHandler")
	portStr := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(portStr, router))
}
