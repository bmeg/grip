package accounts

//"context"
//"github.com/bmeg/grip/gdbi"

type Operation string

const (
	Query       Operation = "query"
	Create      Operation = "create"
	Update      Operation = "update"
	Submit      Operation = "submit"
	QueryRepeat Operation = "query_repeat"
)

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
