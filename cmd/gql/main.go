package gql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bmeg/grip/log"
	"github.com/spf13/cobra"
)

var host = "localhost:8201"
var verbose bool

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "gql <graph> <GQL query>",
	Short: "Query a graph using GQL",
	Long: `Query a graph using GQL.
Example:
    grip query example-graph 'match (n:Person {name: "Bob"}) return n'`,

	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]
		queryString := args[1]

		if verbose {
			c := log.DefaultLoggerConfig()
			c.Level = "debug"
			log.ConfigureLogger(c)
		}

		queryMap := map[string]string{"query": queryString}
		payload, err := json.Marshal(queryMap)
		if err != nil {
			return fmt.Errorf("marshaling query: %v", err)
		}
		url := fmt.Sprintf("http://%s/gql/%s", host, graph)
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
		if err != nil {
			return fmt.Errorf("building request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			return fmt.Errorf("making request: %v", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading response body: %v", err)
		}

		fmt.Printf("%s\n", body)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVarP(&host, "host", "H", host, "Grip server host:port")
	Cmd.Flags().BoolVarP(&verbose, "verbose", "v", verbose, "Enable verbose logging")
}
