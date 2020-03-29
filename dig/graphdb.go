package dig

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

type TabularGDB struct {
	graphs map[string]*TabularGraph
}

func NewGDB(conf Config, configPath string) (*TabularGDB, error) {
	out := TabularGDB{map[string]*TabularGraph{}}
	for k, v := range conf.Graphs {
		fPath := filepath.Join(filepath.Dir(configPath), v)
		if gConf, err := LoadConfig(fPath); err == nil {
			o, err := NewTabularGraph(*gConf)
			if err == nil {
				out.graphs[k] = o
			} else {
				log.Printf("Error loading graph config: %s", err)
			}
		} else {
			log.Printf("Error loading config: %s", err)
		}
	}
	return &out, nil
}

func (g *TabularGDB) AddGraph(string) error {
	return fmt.Errorf("AddGraph not implemented")
}

func (g *TabularGDB) DeleteGraph(string) error {
	return fmt.Errorf("DeleteGraph not implemented")
}

func (g *TabularGDB) ListGraphs() []string {
	out := []string{}
	for i := range g.graphs {
		out = append(out, i)
	}
	return out
}

func (g *TabularGDB) Graph(graphID string) (gdbi.GraphInterface, error) {
	if i, ok := g.graphs[graphID]; ok {
		return i, nil
	}
	return nil, fmt.Errorf("Graph %s not found", graphID)
}

func (g *TabularGDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {
	return nil, fmt.Errorf("BuildSchema not implemented")
}

func (g *TabularGDB) Close() error {
	for _, v := range g.graphs {
		return v.Close()
	}
	return nil
}
