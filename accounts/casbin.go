package accounts

import (
	"fmt"

	"github.com/casbin/casbin/v2"
)

type CasbinAccess struct {
	Model     string
	Policy    string
	encforcer *casbin.Enforcer
}

func (ce *CasbinAccess) init() {
	if ce.encforcer == nil {
		if e, err := casbin.NewEnforcer(ce.Model, ce.Policy); err == nil {
			ce.encforcer = e
		} else {
			fmt.Printf("Casbin Error: %s", err)
		}
	}
}

func (ce *CasbinAccess) Enforce(user string, graph string, operation Operation) error {
	ce.init()
	fmt.Printf("Casbin request '%s' '%s' '%s'\n", user, graph, operation)
	if res, err := ce.encforcer.Enforce(user, graph, string(operation)); res {
		return nil
	} else if err != nil {
		fmt.Printf("casbin error: %s\n", err)
	}
	fmt.Printf("Not allowed: %#v\n", ce.encforcer.GetPolicy())
	//roles, _ := ce.encforcer.GetRolesForUser(user)
	//fmt.Printf("%#v\n", roles)
	return fmt.Errorf("action restricted")
}
