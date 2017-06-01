package rdf

import (
	"io"
	"log"
	"os"
	//"fmt"
	"compress/gzip"
	"github.com/knakk/rdf"
	"github.com/spf13/cobra"
	"github.com/bmeg/arachne/aql"
)


func LoadRDFCmd(cmd *cobra.Command, args []string) error {
	f, err := os.Open(args[0])
	if err != nil {
		log.Printf("Error: %s", err)
		os.Exit(1)
	}
	server := args[0]
	conn, err := aql.Connect(server)
	if err != nil {
		log.Printf("%s", err)
		os.Exit(1)
	}

	vert_map := map[string]int{}

	count := 0
	fz, _ := gzip.NewReader(f)
	dec := rdf.NewTripleDecoder(fz, rdf.RDFXML)
	var cur_query *aql.QueryBuilder = nil
	cur_subj := ""
	for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
		subj := triple.Subj.String()
		if subj != cur_subj && cur_query != nil {
			cur_query.Run()
			cur_query = nil
		}
		cur_subj = subj
		if _, ok := vert_map[subj]; !ok {
			aql.Query(conn).AddV(subj).Run()
			vert_map[subj] = 1
		}
		if triple.Obj.Type() == rdf.TermLiteral {
			//aql.Query(conn).V(subj).Property(triple.Pred.String(), triple.Obj.String()).Run()
			if cur_query == nil {
				a := aql.Query(conn).V(subj)
				cur_query = &a
			}
			b := cur_query.Property(triple.Pred.String(), triple.Obj.String())
			cur_query = &b
		} else {
			obj := triple.Obj.String()
			if _, ok := vert_map[obj]; !ok {
				aql.Query(conn).AddV(obj).Run()
				vert_map[obj] = 1
			}
			aql.Query(conn).V(subj).AddE(triple.Pred.String()).To(obj).Run()
		}
		if count%1000 == 0 {
			log.Printf("Processed %d triples", count)
		}
		count++
	}
	if cur_query != nil {
		cur_query.Run()
	}
	return nil
}


var Cmd = &cobra.Command{
	Use: "rdf",
	Short: "Loads RDF data",
	Long: ``,
	RunE: LoadRDFCmd,
}
