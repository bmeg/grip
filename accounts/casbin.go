package accounts

import (
	"fmt"

	"github.com/bmeg/grip/log"
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
			log.Errorf("Casbin Error: %s", err)
		}
	}
}

func (ce *CasbinAccess) Enforce(user string, graph string, operation Operation) error {
	ce.init()
	log.Infof("Casbin request '%s' '%s' '%s'\n", user, graph, operation)
	if res, err := ce.encforcer.Enforce(user, graph, string(operation)); res {
		return nil
	} else if err != nil {
		log.Errorf("casbin error: %s\n", err)
	}
	log.Errorf("Not allowed: '%s' '%s' '%s'\n", user, graph, operation)
	return fmt.Errorf("action restricted")
}
