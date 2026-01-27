package teorm

import (
	"database/sql"
	"fmt"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// DB is the main struct for teorm
type DB struct {
	DB           *sql.DB
	Statement    *Statement
	Error        error
	RowsAffected int64
}

// Open initializes a new DB connection
func Open(dsn string) (*DB, error) {
	// Using taosRestful driver as port 6041 implies REST interface
	db, err := sql.Open("taosRestful", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{
		DB:        db,
		Statement: &Statement{},
	}, nil
}

// getInstance returns a new DB instance for chaining
func (db *DB) getInstance() *DB {
	return &DB{
		DB:        db.DB,
		Statement: db.Statement.Clone(),
		Error:     db.Error,
	}
}

func (db *DB) AddError(err error) error {
	if db.Error == nil {
		db.Error = err
	} else if err != nil {
		db.Error = fmt.Errorf("%v; %w", db.Error, err)
	}
	return db.Error
}

func (db *DB) Exec(sql string, args ...interface{}) *DB {
	tx := db.getInstance()
	res, err := tx.DB.Exec(sql, args...)
	if err != nil {
		tx.AddError(err)
		return tx
	}
	rows, err := res.RowsAffected()
	if err != nil {
		tx.AddError(err)
		return tx
	}
	tx.RowsAffected = rows
	return tx
}
