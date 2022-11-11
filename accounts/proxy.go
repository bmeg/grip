package accounts

import "fmt"

type ProxyAuth struct {
	Field string
}

func (ba ProxyAuth) Validate(md MetaData) (string, error) {
	if field, ok := md[ba.Field]; ok {
		if len(field) > 0 {
			return field[0], nil
		}
	}
	return "", fmt.Errorf("Field %s empty", ba.Field)
}
