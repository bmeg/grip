package job


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
	Use:   "job",
	Short: "List operations",
}

var listJobsCmd = &cobra.Command{
	Use:   "list",
	Short: "List jobs",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
    graph := args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		resp, err := conn.ListJobs(graph)
		if err != nil {
			return err
		}

		for _, g := range resp {
			fmt.Printf("%s\n", g.Id)
		}
		return nil
	},
}

var dropCmd = &cobra.Command{
	Use:   "drop",
	Short: "List graphs",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
    graph := args[0]
    jobID := args[1]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		resp, err := conn.DeleteJob(graph, jobID)
		if err != nil {
			return err
		}
    fmt.Printf("%s\n", resp)
		return nil
	},
}

var getCmd = &cobra.Command{
	Use:   "get job",
	Short: "Get job info",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
    graph := args[0]
    jobID := args[1]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		resp, err := conn.GetJob(graph, jobID)
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
			return fmt.Errorf("failed to marshal job response: %v", err)
		}
		fmt.Printf("%s\n", string(txt))
		return nil
	},
}

func init() {
	listJobsCmd.Flags().StringVar(&host, "host", host, "grip server url")
	getCmd.Flags().StringVar(&host, "host", host, "grip server url")
	dropCmd.Flags().StringVar(&host, "host", host, "grip server url")

	Cmd.AddCommand(listJobsCmd)
	Cmd.AddCommand(getCmd)
	Cmd.AddCommand(dropCmd)
}
