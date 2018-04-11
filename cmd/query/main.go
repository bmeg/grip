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
  function process(l) {
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
    V: function(id) {
      this.query.push({'v': process(id)})
      return this
    },
    E: function(id) {
      this.query.push({'e': process(id)})
      return this
    },
    outgoing: function(label) {
      this.query.push({'out': process(label)})
      return this
    },
    incoming: function(label) {
      this.query.push({'in': process(label)})
      return this
    },
    both: function(label) {
      this.query.push({'both': process(label)})
      return this
    },
    outgoingEdge: function(label) {
      this.query.push({'out_edge': process(label)})
      return this
    },
    incomingEdge: function(label) {
      this.query.push({'in_edge': process(label)})
      return this
    },
    bothEdge: function(label) {
      this.query.push({'both_edge': process(label)})
      return this
    },
		mark: function(name) {
			this.query.push({'as': name})
			return this
		},
		select: function(marks) {
			this.query.push({'select': {'labels': process(marks)}})
			return this
		},
		hasId: function(id) {
			this.query.push({'has_id': process(id)})
			return this
		},
		hasLabel: function(label) {
			this.query.push({'has_label': process(label)})
			return this
		},
		has: function(key, val) {
			this.query.push({'has': {'key': key, 'within': process(val)}})
			return this
		},
		values: function(v) {
			this.query.push({'values': {'labels': process(v)}})
			return this
		},
    limit: function(n) {
      this.query.push({'limit': n})
      return this
    },
    range: function(begin, end) {
      this.query.push({'begin': b, 'end': e})
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
		distinct: function(val) {
			this.query.push({'distinct': process(val)})
			return this
		},
		render: function(r) {
			this.query.push({'render': r})
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
		val, err := vm.RunString(queryString)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}
		queryJSON, _ := json.Marshal(val)
		query := aql.GraphQuery{}

		err = jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}
		query.Graph = args[0]

		conn, err := aql.Connect(host, true)
		if err != nil {
			log.Printf("Error: %s", err)
			return err
		}

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
