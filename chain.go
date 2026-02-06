package teorm

func (db *DB) Where(query string, args ...interface{}) *DB {
	tx := db.getInstance()
	tx.Statement.Conditions = append(tx.Statement.Conditions, query)
	tx.Statement.Args = append(tx.Statement.Args, args...)
	return tx
}

func (db *DB) Limit(limit int) *DB {
	tx := db.getInstance()
	tx.Statement.LimitVal = limit
	return tx
}

func (db *DB) Offset(offset int) *DB {
	tx := db.getInstance()
	tx.Statement.OffsetVal = offset
	return tx
}

func (db *DB) Select(query interface{}, args ...interface{}) *DB {
	tx := db.getInstance()
	// Simplify: assume query is string for now
	if str, ok := query.(string); ok {
		tx.Statement.Selects = append(tx.Statement.Selects, str)
	}
	return tx
}

func (db *DB) Order(value interface{}) *DB {
	tx := db.getInstance()
	if str, ok := value.(string); ok {
		tx.Statement.Order = str
	}
	return tx
}

func (db *DB) Group(value interface{}) *DB {
	tx := db.getInstance()
	if str, ok := value.(string); ok {
		tx.Statement.Group = str
	}
	return tx
}
