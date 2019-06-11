package gen3

import (
	"crypto/md5"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/bmeg/grip/util"
	"github.com/jmoiron/sqlx"
	//log "github.com/sirupsen/logrus"
)

type edgeDef struct {
	table    string
	label    string
	dstLabel string
	srcLabel string
	backref  bool
}

type vertexDef struct {
	table string
	label string
	out   map[string][]*edgeDef
	in    map[string][]*edgeDef
}

type graphConfig struct {
	// vertex label to vertexDef
	vertices map[string]*vertexDef
	// edge label to edgeDefs
	edges map[string][]*edgeDef
	// table name to vertex def
	vertexTables map[string]*vertexDef
	// table name to edge def
	edgeTables map[string]*edgeDef
}

// list vertex postgres tables
func (gc *graphConfig) listVertexTables() []string {
	tables := []string{}
	for k := range gc.vertexTables {
		tables = append(tables, k)
	}
	return tables
}

// list edge postgres tables
func (gc *graphConfig) listEdgeTables() []string {
	tables := []string{}
	for k := range gc.edgeTables {
		tables = append(tables, k)
	}
	return tables
}

// list all postgres tables
func (gc *graphConfig) listTables() []string {
	return append(gc.listVertexTables(), gc.listEdgeTables()...)
}

// get outgoing edgeDefs
func (gc *graphConfig) out(label string) map[string][]*edgeDef {
	if val, ok := gc.vertices[label]; ok {
		return val.out
	}
	return make(map[string][]*edgeDef)
}

// get incoming edgeDefs
func (gc *graphConfig) in(label string) map[string][]*edgeDef {
	if val, ok := gc.vertices[label]; ok {
		return val.in
	}
	return make(map[string][]*edgeDef)
}

// lookup label by tablename
func (gc *graphConfig) label(table string) string {
	label := ""
	if val, ok := gc.vertexTables[table]; ok {
		label = val.label
	} else if val, ok := gc.edgeTables[table]; ok {
		label = val.label
	}
	return label
}

// lookup tablename by label
func (gc *graphConfig) table(label string) string {
	table := ""
	if val, ok := gc.vertices[label]; ok {
		table = val.table
	} else if val, ok := gc.edges[label]; ok {
		if len(val) == 1 {
			table = val[0].table
		}
	}
	return table
}

// read the schema files to determine the layout of the postgres database
func getGraphConfig(schemaDir string, exclude []string) (*graphConfig, error) {
	schemas, err := loadAllSchemas(schemaDir, exclude)
	if err != nil {
		return nil, err
	}
	g := &graphConfig{
		vertices:     make(map[string]*vertexDef),
		vertexTables: make(map[string]*vertexDef),
		edges:        make(map[string][]*edgeDef),
		edgeTables:   make(map[string]*edgeDef),
	}

	// initialize vertex objects
	for label := range schemas {
		g.vertices[label] = &vertexDef{
			table: vertexTablename(label),
			label: label,
			out:   make(map[string][]*edgeDef),
			in:    make(map[string][]*edgeDef),
		}
	}

	// add edge info
	for srcLabel, data := range schemas {
		for _, link := range data.Links {
			eDef := &edgeDef{
				table:    edgeTablename(srcLabel, link.Label, link.TargetType),
				label:    link.Label,
				srcLabel: srcLabel,
				dstLabel: link.TargetType,
				backref:  false,
			}
			g.edges[link.Label] = append(g.edges[link.Label], eDef)
			bRef := &edgeDef{
				table:    edgeTablename(srcLabel, link.Label, link.TargetType),
				label:    link.Backref,
				srcLabel: srcLabel,
				dstLabel: link.TargetType,
				backref:  true,
			}
			g.edges[link.Backref] = append(g.edges[link.Backref], bRef)
			g.vertices[srcLabel].out[link.Label] = append(g.vertices[srcLabel].out[link.Label], eDef)
			g.vertices[srcLabel].in[link.Backref] = append(g.vertices[srcLabel].in[link.Backref], bRef)
			g.vertices[link.TargetType].out[link.Backref] = append(g.vertices[link.TargetType].out[link.Backref], bRef)
			g.vertices[link.TargetType].in[link.Label] = append(g.vertices[link.TargetType].in[link.Label], eDef)
		}
	}
	// fill in table lookups
	for _, v := range g.edges {
		for _, e := range v {
			g.edgeTables[e.table] = e
		}
	}
	for _, v := range g.vertices {
		g.vertexTables[v.table] = v
	}
	return g, nil
}

func vertexTablename(label string) string {
	return fmt.Sprintf(
		"node_%s",
		strings.ReplaceAll(label, "_", ""),
	)
}

// https://github.com/uc-cdis/gdcdatamodel/blob/7aacbe2f383234b2ad4cb28418cb2f00dd2d24f7/gdcdatamodel/models/__init__.py#L370
func edgeTablename(srcLabel, label, dstLabel string) string {
	tablename := fmt.Sprintf(
		"edge_%s%s%s",
		strings.ReplaceAll(srcLabel, "_", ""),
		strings.ReplaceAll(label, "_", ""),
		strings.ReplaceAll(dstLabel, "_", ""),
	)

	if len(tablename) <= 40 {
		return tablename
	}

	truncSrc := []string{}
	truncLabel := []string{}
	truncDst := []string{}
	for _, x := range strings.Split(srcLabel, "_") {
		truncSrc = append(truncSrc, x[:2])
	}
	for _, x := range strings.Split(label, "_") {
		truncLabel = append(truncLabel, x[:2])
	}
	for _, x := range strings.Split(dstLabel, "_") {
		truncDst = append(truncDst, x[:2])
	}

	return fmt.Sprintf(
		"edge_%s_%s",
		fmt.Sprintf("%x", md5.Sum([]byte(tablename)))[:8],
		fmt.Sprintf(
			"%s%s%s", strings.Join(truncSrc, ""),
			strings.Join(truncLabel, ""),
			strings.Join(truncDst, ""),
		),
	)
}

// read the schema files and create tables in a postgres database
// doesn't create contraints on tables like psqlgraph does
// its a close enough approximation for testing
func setupDatabase(conf Config) error {
	schemas, err := loadAllSchemas(conf.SchemaDir)
	if err != nil {
		return err
	}

	connString, err := util.BuildPostgresConnStr(
		conf.Host, conf.Port, conf.User, conf.Password, conf.DBName, conf.SSLMode,
	)
	if err != nil {
		return err
	}
	db, err := sqlx.Connect("postgres", connString)
	if err != nil {
		if dbDoesNotExist(err) {
			err = util.CreatePostgresDatabase(
				conf.Host, conf.Port, conf.User, conf.Password, conf.DBName, conf.SSLMode,
			)
			if err != nil {
				return err
			}
			return setupDatabase(conf)
		}
		return fmt.Errorf("connecting to database: %v", err)
	}
	defer db.Close()

	var stmts []string
	var table, stmt string
	for label, data := range schemas {
		table = vertexTablename(label)
		stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ", table) +
			"(created timestamp with time zone NOT NULL DEFAULT (current_timestamp), " +
			"acl text[], _sysan jsonb, _props jsonb, node_id text PRIMARY KEY NOT NULL)"
		stmts = append(stmts, stmt)
		stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s__sysan_idx ON %s using gin (_sysan)", table, table)
		stmts = append(stmts, stmt)
		stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s__props_idx ON %s using gin (_props)", table, table)
		stmts = append(stmts, stmt)
		stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s__sysan___props_idx ON %s using gin (_sysan, _props)", table, table)
		stmts = append(stmts, stmt)
		for _, link := range data.Links {
			table = edgeTablename(label, link.Label, link.TargetType)
			stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ", table) +
				"(created timestamp with time zone NOT NULL DEFAULT (current_timestamp), " +
				"acl text[], _sysan jsonb, _props jsonb, " +
				"src_id text NOT NULL, dst_id text NOT NULL, " +
				"PRIMARY KEY (src_id, dst_id))"
			stmts = append(stmts, stmt)
			stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_src_id_idx ON %s using btree (src_id)", table, table)
			stmts = append(stmts, stmt)
			stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_dst_id_idx ON %s using btree (dst_id)", table, table)
			stmts = append(stmts, stmt)
		}
	}
	for _, s := range stmts {
		_, err = db.Exec(s)
		if err != nil {
			return fmt.Errorf("executing statement: %s\n %v", s, err)
		}
	}
	return nil
}

func insertNode(db *sqlx.DB, table, id string) error {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	stmt, args, err := psql.Insert(table).Columns("node_id").
		Values(id).
		Suffix("ON CONFLICT DO NOTHING").
		ToSql()
	if err != nil {
		return fmt.Errorf("insertNode: creating statement: %v", err)
	}
	_, err = db.Exec(stmt, args...)
	if err != nil {
		return fmt.Errorf("insertNode: exec: %v", err)
	}
	return nil
}

func insertEdge(db *sqlx.DB, table, srcID, dstID string) error {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	stmt, args, err := psql.Insert(table).Columns("src_id", "dst_id").
		Values(srcID, dstID).
		Suffix("ON CONFLICT DO NOTHING").
		ToSql()
	if err != nil {
		return fmt.Errorf("insertEdge: creating statement: %v", err)
	}
	_, err = db.Exec(stmt, args...)
	if err != nil {
		return fmt.Errorf("insertEdge: exec: %v", err)
	}
	return nil
}

func createTestData(conf Config) error {
	connString, err := util.BuildPostgresConnStr(
		conf.Host, conf.Port, conf.User, conf.Password, conf.DBName, conf.SSLMode,
	)
	if err != nil {
		return err
	}
	db, err := sqlx.Connect("postgres", connString)
	if err != nil {
		return fmt.Errorf("connecting to database: %v", err)
	}
	defer db.Close()

	err = insertNode(db, "node_program", "program-1")
	if err != nil {
		return err
	}

	err = insertNode(db, "node_project", "project-1")
	if err != nil {
		return err
	}

	err = insertEdge(db, "edge_projectmemberofprogram", "project-1", "program-1")
	if err != nil {
		return err
	}

	err = insertNode(db, "node_experiment", "experiment-1")
	if err != nil {
		return err
	}

	err = insertEdge(db, "edge_experimentperformedforproject", "experiment-1", "project-1")
	if err != nil {
		return err
	}

	cases := []string{"case-1", "case-2", "case-3", "case-4"}
	for _, c := range cases {
		err = insertNode(db, "node_case", c)
		if err != nil {
			return err
		}
		err = insertEdge(db, "edge_casememberofexperiment", c, "experiment-1")
		if err != nil {
			return err
		}
	}

	return nil
}
