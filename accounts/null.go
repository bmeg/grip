package accounts

import (
	"fmt"
)

type NullAuth struct{}

type NullAccess struct{}

func (na NullAuth) Validate(md MetaData) (string, error) {
	return "", nil
}

func (be NullAccess) Enforce(user string, graph string, operation Operation) error {
	fmt.Printf("Enforce: %s %s %#v\n", user, graph, operation)
	return nil
}