package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/grpc"
)

const (
	defaultMaxSteps    = 64
	defaultMaxInflight = 8
	defaultMaxBatch    = 512
	maxTravelersPerMsg = 100000
)

// Options configures a resource node server.
type Options struct {
	// Name identifies the source.
	Name string
	// Prefix is the CURIE prefix of every vertex and edge ID in the graph, e.g. "Character:".
	Prefix string
	// Schema describes labels and fields. If nil it is built from the graph's label lists.
	Schema *gripql.Graph
	// EdgeLinks lists stored edges that cross prefixes (see ScanEdgeLinks).
	EdgeLinks []*gripper.EdgeLink
	// IndexedFields are advisory "label.field" names.
	IndexedFields []string
	// MaxSteps limits the number of steps in a segment (default 64).
	MaxSteps int
	// MaxInflight limits the number of travelers batches processed concurrently per stream (default 8).
	MaxInflight int
	// MaxBatch is the maximum number of results sent in a single message (default 512).
	MaxBatch int
	// WorkDir is used for temporary resources needed by processors.
	WorkDir string
}

// Server implements gripper.GRIPSourceServer on top of any gdbi.GraphInterface.
type Server struct {
	gripper.UnimplementedGRIPSourceServer
	graph    gdbi.GraphInterface
	boundary *BoundaryGraph
	opts     Options
}

// NewServer creates a resource node serving graph. All vertex and edge IDs in the
// graph must start with opts.Prefix.
func NewServer(graph gdbi.GraphInterface, opts Options) (*Server, error) {
	if opts.Prefix == "" {
		return nil, fmt.Errorf("node prefix is required")
	}
	if opts.MaxSteps <= 0 {
		opts.MaxSteps = defaultMaxSteps
	}
	if opts.MaxInflight <= 0 {
		opts.MaxInflight = defaultMaxInflight
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = defaultMaxBatch
	}
	return &Server{graph: graph, boundary: NewBoundaryGraph(graph, opts.Prefix), opts: opts}, nil
}

// GetInfo describes the served subgraph.
func (s *Server) GetInfo(ctx context.Context, _ *gripper.Empty) (*gripper.SourceInfo, error) {
	schema := s.opts.Schema
	if schema == nil {
		schema = &gripql.Graph{}
		if labels, err := s.graph.ListVertexLabels(); err == nil {
			for _, l := range labels {
				schema.Vertices = append(schema.Vertices, &gripql.Vertex{Id: l, Label: l})
			}
		} else {
			log.Errorf("GetInfo: listing vertex labels: %s", err)
		}
		if labels, err := s.graph.ListEdgeLabels(); err == nil {
			for _, l := range labels {
				schema.Edges = append(schema.Edges, &gripql.Edge{Id: l, Label: l})
			}
		}
	}
	return &gripper.SourceInfo{
		ProtocolVersion: gripper.ProtocolVersion,
		Name:            s.opts.Name,
		Prefix:          s.opts.Prefix,
		Schema:          schema,
		EdgeLinks:       s.opts.EdgeLinks,
		IndexedFields:   s.opts.IndexedFields,
		MaxBatch:        uint32(s.opts.MaxBatch),
	}, nil
}

// Register adds the server to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	gripper.RegisterGRIPSourceServer(g, s)
}

// CuriePrefix returns the part of an ID up to and including the first colon.
func CuriePrefix(id string) string {
	if i := strings.Index(id, ":"); i >= 0 {
		return id[:i+1]
	}
	return id
}

// ScanEdgeLinks inspects the edges stored in graph and returns one link for every
// (label, from prefix, to prefix) combination that is not entirely inside prefix.
// It reads every vertex ID, so it is meant to run once at startup.
func ScanEdgeLinks(ctx context.Context, graph gdbi.GraphInterface, prefix string) ([]*gripper.EdgeLink, error) {
	ids := []string{}
	for v := range graph.GetVertexList(ctx, false) {
		ids = append(ids, v.ID)
	}
	req := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(req)
		for _, id := range ids {
			req <- gdbi.ElementLookup{ID: id, Ref: &gdbi.BaseTraveler{}}
		}
	}()
	seen := map[[3]string]bool{}
	out := []*gripper.EdgeLink{}
	for el := range graph.GetOutEdgeChannel(ctx, req, false, false, nil) {
		if el.Edge == nil || el.IsSignal() {
			continue
		}
		e := el.Edge.Get()
		if strings.HasPrefix(e.From, prefix) && strings.HasPrefix(e.To, prefix) {
			continue
		}
		l := &gripper.EdgeLink{Label: e.Label, FromPrefix: CuriePrefix(e.From), ToPrefix: CuriePrefix(e.To)}
		key := [3]string{l.Label, l.FromPrefix, l.ToPrefix}
		if !seen[key] {
			seen[key] = true
			out = append(out, l)
		}
	}
	return out, ctx.Err()
}
