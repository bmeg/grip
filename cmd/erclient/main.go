package erclient

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripper"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:50051"
var source = "main"
var dataOnly = false

func getClient() *gripper.GripperClient {
	c := map[string]gripper.DriverConfig{source: {Host: host}}
	out := gripper.NewGripperClient(c)
	return out
}

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "er",
	Short: "Issue command to Grip External Resource",
}

var ListCmd = &cobra.Command{
	Use:   "list",
	Short: "List collections provided by external resource",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := getClient()
		for name := range client.GetCollections(context.Background(), source) {
			fmt.Printf("%s\n", name)
		}
		return nil
	},
}

var InfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Get info about a collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := getClient()
		collection := args[0]
		out, err := client.GetCollectionInfo(context.Background(), source, collection)
		if err != nil {
			return err
		}
		jm := protojson.MarshalOptions{}
		fmt.Printf("%s\n", jm.Format(out))
		return nil
	},
}

var RowsCmd = &cobra.Command{
	Use:   "rows",
	Short: "List rows from a collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := getClient()
		collection := args[0]
		jm := protojson.MarshalOptions{}
		for row := range client.GetRows(context.Background(), source, collection) {
			if dataOnly {
				fmt.Printf("%s\n", jm.Format(row.Data))
			} else {
				fmt.Printf("%s\t%s\n", row.Id, jm.Format(row.Data))
			}
		}
		return nil
	},
}

var IdsCmd = &cobra.Command{
	Use:   "ids",
	Short: "List ids from a collection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := getClient()
		collection := args[0]
		for i := range client.GetIDs(context.Background(), source, collection) {
			fmt.Printf("%s\n", i)
		}
		return nil
	},
}

var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "List rows with field match",
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := getClient()
		collection := args[0]
		field := args[1]
		value := args[2]
		jm := protojson.MarshalOptions{}
		rows, err := client.GetRowsByField(context.Background(), source, collection, field, value)
		if err != nil {
			return err
		}
		for row := range rows {
			if dataOnly {
				fmt.Printf("%s\n", jm.Format(row.Data))
			} else {
				fmt.Printf("%s\t%s\n", row.Id, jm.Format(row.Data))
			}
		}
		return nil
	},
}

var GetCmd = &cobra.Command{
	Use:   "get",
	Short: "List rows with field match",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := getClient()
		collection := args[0]
		ids := args[1:]

		idChan := make(chan *gripper.RowRequest, 10)

		rows, err := client.GetRowsByID(context.Background(), source, collection, idChan)
		if err != nil {
			return err
		}

		go func() {
			defer close(idChan)
			for _, i := range ids {
				r := gripper.RowRequest{Id: i}
				idChan <- &r
			}
		}()
		jm := protojson.MarshalOptions{}
		for row := range rows {
			if dataOnly {
				fmt.Printf("%s\n", jm.Format(row.Data))
			} else {
				fmt.Printf("%s\t%s\n", row.Id, jm.Format(row.Data))
			}
		}
		return nil
	},
}

func init() {
	rFlags := RowsCmd.Flags()
	rFlags.BoolVarP(&dataOnly, "data", "d", false, "Only Show data")

	qFlags := QueryCmd.Flags()
	qFlags.BoolVarP(&dataOnly, "data", "d", false, "Only Show data")

	gFlags := GetCmd.Flags()
	gFlags.BoolVarP(&dataOnly, "data", "d", false, "Only Show data")

	Cmd.AddCommand(ListCmd)
	Cmd.AddCommand(InfoCmd)
	Cmd.AddCommand(RowsCmd)
	Cmd.AddCommand(IdsCmd)
	Cmd.AddCommand(QueryCmd)
	Cmd.AddCommand(GetCmd)
	Cmd.PersistentFlags().StringVar(&host, "host", host, "gripper server url")
}
