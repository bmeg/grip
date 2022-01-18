package accounts

import (
	"github.com/casbin/casbin"
)

type CasbinAccess struct {
	encforcer *casbin.Enforcer
}

func NewCasbinAccess(modelPath string, policyPath string) Access {
	e := casbin.NewEnforcer(modelPath, policyPath)
	return &CasbinAccess{e}
}

func (ce *CasbinAccess) Enforce(user string, graph string, operation Operation) error {
	return nil
}

/*
  if res := e.Enforce("alice", "data2", "write"); res {
    fmt.Printf("Permitted\n")
  } else {
    fmt.Printf("Restricted\n")
  }


  if res := e.Enforce("bob", "data2", "read"); res {
    fmt.Printf("Permitted\n")
  } else {
    fmt.Printf("Restricted\n")
  }
*/
