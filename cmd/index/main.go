package index

import (
	"fmt"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
  "github.com/golang/protobuf/jsonpb"
)

var host = "localhost:8202"
var limit = -1
// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "index",
	Short: "List operations",
}

var indexCreateCmd = &cobra.Command{
	Use:   "create <graph> <column>",
	Short: "create index",
	Long:  ``,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		err = conn.AddIndex(args[0], args[1])
		if err != nil {
			return err
		}
		return nil
	},
}

var indexDropCmd = &cobra.Command{
	Use:   "drop <graph> <column>",
	Short: "drop an index",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
    conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		err = conn.DeleteIndex(args[0], args[1])
		if err != nil {
			return err
		}
		return nil
	},
}

var indexListCmd = &cobra.Command{
	Use:   "list <graph>",
	Short: "list indexes for a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
    conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		resp, err := conn.ListIndices(args[0])
		if err != nil {
			return err
		}
    for _, i := range resp {
      fmt.Printf("%s\n", i)
    }
		return nil
	},
}

var indexSearchCmd = &cobra.Command{
  Use:   "search <graph> <column> <term>",
  Short: "search the index for a term",
  Long:  ``,
  Args:  cobra.ExactArgs(3),
  RunE: func(cmd *cobra.Command, args []string) error {
    conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
    if err != nil {
      return err
    }

    query := gripql.Index([]string{args[1]}, args[2])
    if limit > 0 {
      query = query.Limit(uint32(limit))
    }
    res, err := conn.Traversal(&gripql.GraphQuery{Graph:args[0], Query:query.Statements})
    if err != nil {
      return err
    }

    marsh := jsonpb.Marshaler{}
    for row := range res {
      rowString, _ := marsh.MarshalToString(row)
      fmt.Printf("%s\n", rowString)
    }
    return nil
  },
}

func init() {
	iflags := indexCreateCmd.Flags()
	iflags.StringVar(&host, "host", host, "grip server url")
  dflags := indexDropCmd.Flags()
  dflags.StringVar(&host, "host", host, "grip server url")
  lflags := indexListCmd.Flags()
  lflags.StringVar(&host, "host", host, "grip server url")

  sflags := indexSearchCmd.Flags()
  sflags.StringVar(&host, "host", host, "grip server url")
  sflags.IntVarP(&limit, "limit", "n", limit, "Limit Count")


	Cmd.AddCommand(indexCreateCmd)
	Cmd.AddCommand(indexDropCmd)
  Cmd.AddCommand(indexSearchCmd)
  Cmd.AddCommand(indexListCmd)
}
