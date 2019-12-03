package util

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func BuildPostgresConnStr(host string, port uint, user, password, dbname, sslmode string) (string, error) {
	if host == "" || port == 0 || user == "" {
		return "", fmt.Errorf("host, port, and user must be defined")
	}
	if sslmode == "" {
		sslmode = "disable"
	}
	connString := fmt.Sprintf(
		"host=%s port=%v user=%s sslmode=%s",
		host, port, user, sslmode,
	)
	if dbname != "" {
		connString = fmt.Sprintf("%s dbname=%s", connString, dbname)
	}
	if password != "" {
		connString = fmt.Sprintf("%s password=%s", connString, password)
	}
	return connString, nil
}

// CreatePosgresDatabase creates a postgres database
func CreatePostgresDatabase(host string, port uint, user, password, dbname, sslmode string) error {
	connString, err := BuildPostgresConnStr(
		host, port, user, password, "", sslmode,
	)
	if err != nil {
		return err
	}
	db, err := sqlx.Connect("postgres", connString)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		return fmt.Errorf("creating database: %v", err)
	}
	return nil
}
