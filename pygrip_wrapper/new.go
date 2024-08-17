package pygrip_wrapper

import (
	"fmt"

	"github.com/bmeg/grip/gdbi"
)

func NewMemServer() gdbi.GraphDB {
	fmt.Printf("I got this far\n")
	//db, _ := leveldb.NewMemKVInterface("", kvi.Options{})
	//graphdb := kvgraph.NewKVGraph(db)
	//return graphdb
	return nil
}
