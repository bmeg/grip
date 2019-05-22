package util

import (
	"fmt"
)

func BuildPostgresConnStr(host string, port uint, user, password, dbname, sslmode string) (string, error) {
	if host == "" || port == 0 || user == "" {
		return "", fmt.Errorf("host, port, user and dbname must be defined")
	}
	if sslmode == "" {
		sslmode = "disable"
	}
	connString := fmt.Sprintf(
		"host=%s port=%v user=%s dbname=%s sslmode=%s",
		host, port, user, dbname, sslmode,
	)
	if password != "" {
		connString = fmt.Sprintf("%s password=%s", connString, password)
	}
	return connString, nil
}
