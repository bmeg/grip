package psqlx

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"github.com/jmoiron/sqlx"
	"google.golang.org/protobuf/types/known/structpb"
)

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type DelegatedExecutor interface {
	Execute(ctx context.Context, graph string, req *DelegatedRequest) ([]*gripql.QueryResult, error)
	Close() error
}

type sqlDelegatedExecutor struct {
	db *sqlx.DB
}

func newSQLDelegatedExecutor(conf Config) (DelegatedExecutor, error) {
	connString, err := util.BuildPostgresConnStr(
		conf.Host,
		conf.Port,
		conf.User,
		conf.Password,
		conf.DBName,
		conf.SSLMode,
	)
	if err != nil {
		return nil, err
	}
	db, err := sqlx.Connect("postgres", connString)
	if err != nil {
		return nil, fmt.Errorf("psqlx: connecting delegated executor database: %v", err)
	}
	return &sqlDelegatedExecutor{db: db}, nil
}

func (e *sqlDelegatedExecutor) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *sqlDelegatedExecutor) Execute(ctx context.Context, graph string, req *DelegatedRequest) ([]*gripql.QueryResult, error) {
	if req == nil {
		return nil, fmt.Errorf("psqlx: delegated request cannot be nil")
	}
	if !identPattern.MatchString(req.Schema) || !identPattern.MatchString(req.Function) {
		return nil, fmt.Errorf("psqlx: invalid extension target %q.%q", req.Schema, req.Function)
	}

	q := fmt.Sprintf("SELECT * FROM %s.%s($1, $2::jsonb)", req.Schema, req.Function)
	rows, err := e.db.QueryxContext(ctx, q, graph, string(req.Query))
	if err != nil {
		return nil, fmt.Errorf("psqlx: delegated execution query failed: %v", err)
	}
	defer rows.Close()

	out := []*gripql.QueryResult{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("psqlx: delegated row scan failed: %v", err)
		}
		decoded, err := decodeDelegatedRow(raw)
		if err != nil {
			return nil, err
		}
		if decoded != nil {
			out = append(out, decoded)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("psqlx: delegated row iteration failed: %v", err)
	}
	return out, nil
}

type delegatedRow struct {
	ResultType   string         `json:"result_type"`
	Count        uint32         `json:"count"`
	Render       interface{}    `json:"render"`
	Path         []interface{}  `json:"path"`
	Vertex       *delegatedNode `json:"vertex"`
	Edge         *delegatedEdge `json:"edge"`
	Aggregations *delegatedAgg  `json:"aggregations"`
	Error        *delegatedErr  `json:"error"`
}

type delegatedNode struct {
	ID    string                 `json:"id"`
	Label string                 `json:"label"`
	Data  map[string]interface{} `json:"data"`
}

type delegatedEdge struct {
	ID    string                 `json:"id"`
	Label string                 `json:"label"`
	From  string                 `json:"from"`
	To    string                 `json:"to"`
	Data  map[string]interface{} `json:"data"`
}

type delegatedAgg struct {
	Name  string      `json:"name"`
	Key   interface{} `json:"key"`
	Value float64     `json:"value"`
}

type delegatedErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func decodeDelegatedRow(raw []byte) (*gripql.QueryResult, error) {
	row := delegatedRow{}
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, fmt.Errorf("psqlx: delegated row decode failed: %v", err)
	}

	if row.Error != nil {
		return nil, fmt.Errorf("psqlx: extension error %s: %s", row.Error.Code, row.Error.Message)
	}

	switch row.ResultType {
	case "count":
		return &gripql.QueryResult{Result: &gripql.QueryResult_Count{Count: row.Count}}, nil

	case "render":
		v, err := structpb.NewValue(row.Render)
		if err != nil {
			return nil, fmt.Errorf("psqlx: invalid render payload: %v", err)
		}
		return &gripql.QueryResult{Result: &gripql.QueryResult_Render{Render: v}}, nil

	case "vertex":
		if row.Vertex == nil {
			return nil, fmt.Errorf("psqlx: vertex row missing vertex payload")
		}
		s, err := structpb.NewStruct(defaultData(row.Vertex.Data))
		if err != nil {
			return nil, fmt.Errorf("psqlx: invalid vertex data payload: %v", err)
		}
		return &gripql.QueryResult{Result: &gripql.QueryResult_Vertex{Vertex: &gripql.Vertex{Id: row.Vertex.ID, Label: row.Vertex.Label, Data: s}}}, nil

	case "edge":
		if row.Edge == nil {
			return nil, fmt.Errorf("psqlx: edge row missing edge payload")
		}
		s, err := structpb.NewStruct(defaultData(row.Edge.Data))
		if err != nil {
			return nil, fmt.Errorf("psqlx: invalid edge data payload: %v", err)
		}
		return &gripql.QueryResult{Result: &gripql.QueryResult_Edge{Edge: &gripql.Edge{Id: row.Edge.ID, Label: row.Edge.Label, From: row.Edge.From, To: row.Edge.To, Data: s}}}, nil

	case "aggregations":
		if row.Aggregations == nil {
			return nil, fmt.Errorf("psqlx: aggregations row missing payload")
		}
		k, err := structpb.NewValue(row.Aggregations.Key)
		if err != nil {
			return nil, fmt.Errorf("psqlx: invalid aggregation key payload: %v", err)
		}
		return &gripql.QueryResult{Result: &gripql.QueryResult_Aggregations{Aggregations: &gripql.NamedAggregationResult{Name: row.Aggregations.Name, Key: k, Value: row.Aggregations.Value}}}, nil

	case "path":
		lv, err := structpb.NewList(row.Path)
		if err != nil {
			return nil, fmt.Errorf("psqlx: invalid path payload: %v", err)
		}
		return &gripql.QueryResult{Result: &gripql.QueryResult_Path{Path: lv}}, nil

	default:
		return nil, fmt.Errorf("psqlx: unsupported delegated result type %q", row.ResultType)
	}
}

func defaultData(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return map[string]interface{}{}
	}
	return in
}
