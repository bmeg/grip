package query

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine/underscore"
	"github.com/dop251/goja"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

var ophionString = `

function query() {
  function labels(l) {
   if (!l) {
     l = []
   } else if (_.isString(l)) {
     l = [l]
   } else if (!_.isArray(l)) {
     console.log("not something we know how to make labels out of:")
     console.log(l)
   }
   return l
  }

  return {
    query: [],
    V: function(l) {
      this.query.push({'v': labels(l)})
      return this
    },
    E: function(l) {
      this.query.push({'e': labels(l)})
      return this
    },
    out: function(l) {
      this.query.push({'out': labels(l)})
      return this
    },
    in: function(l) {
      this.query.push({'in': labels(l)})
      return this
    },
    outE: function(l) {
      this.query.push({'out_edge': labels(l)})
      return this
    },
    inE: function(l) {
      this.query.push({'in_edge': labels(l)})
      return this
    },
    limit: function(l) {
      this.query.push({'limit': l})
      return this
    },
		groupCount: function(field) {
			this.query.push({'group_count': field})
			return this
		},
		count: function() {
			this.query.push({'count': ''})
			return this
	  },
		hasLabel: function(l) {
			this.query.push({'has_label': labels(l)})
			return this
		}
  }
}

O = {
    query : query
}
`

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "query",
	Short: "Run query on Arachne Server",
	Long:  ``,
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		vm := goja.New()

		us, _ := underscore.Asset("underscore.js")
		if _, err := vm.RunString(string(us)); err != nil {
			return err
		}

		if _, err := vm.RunString(ophionString); err != nil {
			log.Printf("Error: %s", err)
			return err
		}
		queryString := args[1]
		//log.Printf("%s\n", queryString)
		val, err := vm.RunString(queryString)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}
		queryJSON, _ := json.Marshal(val)
		query := aql.GraphQuery{}
		jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
		err = jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}

		conn, err := aql.Connect(host, true)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}
		query.Graph = args[0]
		res, err := conn.Traversal(&query)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}

		marsh := jsonpb.Marshaler{}
		for row := range res {
			rowString, _ := marsh.MarshalToString(row)
			fmt.Printf("%s\n", rowString)
		}

		return nil
	}}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
}
