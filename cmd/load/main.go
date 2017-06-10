package load

import (
	"log"
	"strings"
	"github.com/spf13/cobra"
	"github.com/bmeg/golib"
	"github.com/golang/protobuf/jsonpb"
  "github.com/bmeg/arachne/aql"
)

var host string = "localhost:9090"
var vertexFile string = ""
var edgeFile string = ""

var Cmd = &cobra.Command{
	Use: "load",
	Short: "Load Data into Arachne Server",
	Long: ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Printf("Loading Data")

		/*
    conn, err := aql.Connect(host)
    if err != nil {
      return err
    }
		*/

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			reader, err := golib.ReadFileLines(vertexFile)
			if err != nil {
				return err
			}
			for line := range reader {
				v := aql.Vertex{}
				jsonpb.Unmarshal(strings.NewReader(string(line)), &v)
				log.Printf("%#v", v)
			}

		}
		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
			reader, err := golib.ReadFileLines(vertexFile)
			if err != nil {
				return err
			}
			for line := range reader {
				v := aql.Edge{}
				jsonpb.Unmarshal(strings.NewReader(string(line)), &v)
				log.Printf("%#v", v)
			}
		}




		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
}
