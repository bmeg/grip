package dig

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bmeg/grip/gripper"

	"github.com/bmeg/grip/gdbi"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/spf13/cobra"

	"encoding/json"

	"github.com/bmeg/grip/gripql"
	gripqljs "github.com/bmeg/grip/gripql/javascript"
	"github.com/bmeg/grip/jsengine/underscore"
	"github.com/dop251/goja"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/util"
)

var idxName string = "table.db"

func ParseQuery(queryString string) (*gripql.GraphQuery, error) {
	vm := goja.New()

	us, err := underscore.Asset("underscore.js")
	if err != nil {
		return nil, fmt.Errorf("failed to load underscore.js")
	}
	if _, err := vm.RunString(string(us)); err != nil {
		return nil, err
	}

	gripqlString, err := gripqljs.Asset("gripql.js")
	if err != nil {
		return nil, fmt.Errorf("failed to load gripql.js")
	}
	if _, err := vm.RunString(string(gripqlString)); err != nil {
		return nil, err
	}

	val, err := vm.RunString(queryString)
	if err != nil {
		return nil, err
	}

	queryJSON, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}

	query := gripql.GraphQuery{}
	err = protojson.Unmarshal(queryJSON, &query)
	if err != nil {
		return nil, err
	}
	return &query, nil
}

func Query(graph gdbi.GraphInterface, query *gripql.GraphQuery) error {

	p, err := graph.Compiler().Compile(query.Query)
	if err != nil {
		return err
	}
	workdir := "./test.workdir." + util.RandomString(6)
	defer os.RemoveAll(workdir)
	res := pipeline.Run(context.Background(), p, workdir)

	for row := range res {
		rowString, _ := protojson.Marshal(row)
		fmt.Printf("%s\n", string(rowString))
	}
	return nil
}

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "dig <config> <query>",
	Short: "Do a single query using the gripper driver",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {

		configFile := args[0]
		queryString := args[1]

		config := gripper.Config{Graph: "main", ConfigFile: configFile}

		gdb, err := gripper.NewGDB(config, "./")
		if err != nil {
			log.Printf("Error loading Graph: %s", err)
			return err
		}

		graph, err := gdb.Graph("main")
		if err != nil {
			log.Printf("%s", err)
			return err
		}

		query, err := ParseQuery(queryString)
		if err != nil {
			log.Printf("%s", err)
			return err
		}
		log.Printf("Query: %#v", query)
		Query(graph, query)

		gdb.Close()
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&idxName, "db", idxName, "Path to index db")
}
