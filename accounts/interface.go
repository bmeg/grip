package accounts

//"context"
//"github.com/bmeg/grip/gdbi"

type Operation string

const (
	Query       Operation = "query"
	Write       Operation = "write"
	Read        Operation = "read"
	Exec        Operation = "exec"
	Admin       Operation = "admin"
	QueryRepeat Operation = "query_repeat"
)

var MethodMap = map[string]Operation{
	"/gripql.Query/Traversal":    Query,
	"/gripql.Query/GetVertex":    Read,
	"/gripql.Query/GetEdge":      Read,
	"/gripql.Query/GetTimestamp": Read,
	"/gripql.Query/GetSchema":    Read,
	"/gripql.Query/GetMapping":   Read,
	"/gripql.Query/ListGraphs":   Read,
	"/gripql.Query/ListIndices":  Read,
	"/gripql.Query/ListLabels":   Read,

	"/gripql.Job/Submit":     Exec,
	"/gripql.Job/ListJobs":   Read,
	"/gripql.Job/SearchJobs": Read,
	"/gripql.Job/DeleteJob":  Write,
	"/gripql.Job/GetJob":     Read,
	"/gripql.Job/ViewJob":    Read,
	"/gripql.Job/ResumeJob":  Exec,

	"/gripql.Edit/AddVertex":    Write,
	"/gripql.Edit/AddEdge":      Write,
	"/gripql.Edit/BulkAdd":      Write,
	"/gripql.Edit/AddGraph":     Write,
	"/gripql.Edit/DeleteGraph":  Write,
	"/gripql.Edit/DeleteVertex": Write,
	"/gripql.Edit/DeleteEdge":   Write,
	"/gripql.Edit/AddIndex":     Write,
	"/gripql.Edit/AddSchema":    Write,
	"/gripql.Edit/AddMapping":   Write,
	"/gripql.Edit/SampleSchema": Write, //Maybe exec?

	"/gripql.Configure/StartPlugin": Admin,
	"/gripql.Configure/ListPlugin":  Admin,
	"/gripql.Configure/ListDrivers": Admin,
}

type MetaData map[string][]string

type Authenticate interface {
	Validate(md MetaData) (string, error)
}

type Access interface {
	Enforce(user string, graph string, operation Operation) error
}

type AuthConfig struct {
	Basic *BasicAuth
}

type AccessConfig struct {
	Null   *NullAccess
	Casbin *CasbinAccess
}

type Config struct {
	Auth   *AuthConfig
	Access *AccessConfig
	auth   Authenticate
	access Access
}
