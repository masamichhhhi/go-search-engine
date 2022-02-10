package gosearchengine

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func NewDBClient(dbConfig *DBConfig) (*sqlx.DB, error) {
	db, err := sqlx.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbConfig.User, dbConfig.Password, dbConfig.Addr, dbConfig.Port, dbConfig.DB),
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type DBConfig struct {
	User     string
	Password string
	Addr     string
	Port     string
	DB       string
}
