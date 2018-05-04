package query

import (
	"encoding/json"
	"fmt"
	//"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine/underscore"
	"github.com/dop251/goja"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

var aqlString = `
function query() {
	function process(l) {
		if (!l) {
			l = []
		} else if (_.isString(l)) {
			l = [l]
		} else if (!_.isArray(l)) {
			throw "not something we know how to make labels out of"
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
		out: function(label) {
			this.query.push({'out': process(label)})
			return this
		},
		in_: function(label) {
			this.query.push({'in': process(label)})
			return this
		},
		both: function(label) {
			this.query.push({'both': process(label)})
			return this
		},
		outE: function(label) {
			this.query.push({'out_edge': process(label)})
			return this
		},
		inE: function(label) {
			this.query.push({'in_edge': process(label)})
			return this
		},
		bothE: function(label) {
			this.query.push({'both_edge': process(label)})
			return this
		},
		as_: function(name) {
			this.query.push({'as': name})
			return this
		},
		select: function(marks) {
			this.query.push({'select': {'labels': process(marks)}})
			return this
		},
		limit: function(n) {
			this.query.push({'limit': n})
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
		},
		where: function(expression) {
			this.query.push({'where': expression})
			return this
		},
		aggregate: function() {
			this.query.push({'aggregate': {'aggregations': Array.prototype.slice.call(arguments)}})
			return this
		}
	}
}

// Where operators
function and_() {
	return {'and': {'expressions': Array.prototype.slice.call(arguments)}}
}

function or_() {
	return {'or': {'expressions': Array.prototype.slice.call(arguments)}}
}

function not_(expression) {
	return {'not': expression}
}

function eq(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'EQ'}}
}

function neq(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'NEQ'}}
}

function gt(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'GT'}}
}

function gte(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'GTE'}}
}

function lt(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'LT'}}
}

function lte(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'LTE'}}
}

function in_(key, values) {
	if (!values) {
		values = []
	} else if (!_.isObject(l) && !_.isArray(l)) {
		values = [values]
	}
	return {'condition': {'key': key, 'value': values, 'condition': 'IN'}}
}

function contains(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'CONTAINS'}}
}

// Aggregation builders
function term(name, label, field, size) {
	agg = {
		"name": name,
		"term": {"label": label, "field": field}
	}
	if (size) {
		if (!_.isNumber(percents)) {
			throw "size expected to be a number"
		}
		agg["term"]["size"] = size
	}
	return agg
}

function percentile(name, label, field, percents) {
	if (!percents) {
		percents = [1, 5, 25, 50, 75, 95, 99]
	} else if (_.isNumber(percents)) {
			percents = [percents]
	} else if (!_.isArray(percents)) {
		throw "percents expected to be an array of numbers"
	}

	return {
		"name": name,
		"percentile": {
			"label": label, "field": field, "percents": percents
		}
	}
}

function histogram(name, label, field, interval) {
	if (interval) {
		if (!_.isNumber(interval)) {
			throw "size expected to be a number"
		}
	}
	return {
		"name": name,
		"histogram": {
			"label": label, "field": field, "interval": interval
		}
	}
}

// base query object
O = {
	query : query
}
`

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "query <graph> <query expression>",
	Short: "Query an Arachne Server",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		vm := goja.New()

		us, _ := underscore.Asset("underscore.js")
		if _, err := vm.RunString(string(us)); err != nil {
			return err
		}

		if _, err := vm.RunString(aqlString); err != nil {
			return err
		}

		queryString := args[1]
		val, err := vm.RunString(queryString)
		if err != nil {
			return err
		}

		queryJSON, err := json.Marshal(val)
		if err != nil {
			return err
		}
		// log.Printf("Query: %s\n", string(queryJSON))

		query := aql.GraphQuery{}
		err = jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
		if err != nil {
			return err
		}
		query.Graph = args[0]

		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		res, err := conn.Traversal(&query)
		if err != nil {
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
