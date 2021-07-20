package list

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List operations",
}

var listTablesCmd = &cobra.Command{
	Use:   "tables",
	Short: "List tables",
	Long:  ``,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		resp, err := conn.ListTables()
		if err != nil {
			return err
		}

		for g := range resp {
			fmt.Printf("%s\t%s\t%s\n", g.Source, g.Name, g.Fields)
		}
		return nil
	},
}

var listGraphsCmd = &cobra.Command{
	Use:   "graphs",
	Short: "List graphs",
	Long:  ``,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		resp, err := conn.ListGraphs()
		if err != nil {
			return err
		}

		for _, g := range resp.Graphs {
			fmt.Printf("%s\n", g)
		}
		return nil
	},
}

var listLabelsCmd = &cobra.Command{
	Use:   "labels <graph>}",
	Short: "List the vertex and edge labels in a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		resp, err := conn.ListLabels(graph)
		if err != nil {
			return err
		}
		m := protojson.MarshalOptions{
			UseEnumNumbers:  false,
			EmitUnpopulated: false,
			Indent:          "  ",
			UseProtoNames:   false,
		}
		txt, err := m.Marshal(resp)
		if err != nil {
			return fmt.Errorf("failed to marshal ListLabels response: %v", err)
		}
		fmt.Printf("%s\n", string(txt))
		return nil
	},
}

func init() {
	listGraphsCmd.Flags().StringVar(&host, "host", host, "grip server url")
	listLabelsCmd.Flags().StringVar(&host, "host", host, "grip server url")
	listTablesCmd.Flags().StringVar(&host, "host", host, "grip server url")

	Cmd.AddCommand(listGraphsCmd)
	Cmd.AddCommand(listLabelsCmd)
	Cmd.AddCommand(listTablesCmd)
}
