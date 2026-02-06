package teorm

import (
	"fmt"
	"reflect"
	"strings"
)

func (db *DB) Find(dest interface{}) *DB {
	tx := db.getInstance()
	schema := Parse(dest)

	tableName := tx.Statement.Table
	if tableName == "" {
		if len(schema.Tags) > 0 {
			// If it's a super table (has tags), query from super table by default
			tableName = schema.Name
		} else if schema.TableName != "" {
			tableName = schema.TableName
		} else {
			tableName = schema.Name
		}
	}

	// Build Select
	selectClause := "*"
	if len(tx.Statement.Selects) > 0 {
		selectClause = strings.Join(tx.Statement.Selects, ", ")
	}

	// Build Where
	whereClause, args := tx.Statement.BuildCondition()

	sql := fmt.Sprintf("SELECT %s FROM %s%s", selectClause, tableName, whereClause)

	if tx.Statement.Order != "" {
		sql += " ORDER BY " + tx.Statement.Order
	}

	if tx.Statement.LimitVal > 0 {
		sql += fmt.Sprintf(" LIMIT %d", tx.Statement.LimitVal)
	}

	if tx.Statement.OffsetVal > 0 {
		sql += fmt.Sprintf(" OFFSET %d", tx.Statement.OffsetVal)
	}

	if tx.Statement.Group != "" {
		sql += " GROUP BY " + tx.Statement.Group
	}

	// Optimization: Inline arguments to avoid driver binding issues
	sql = Explain(sql, args...)

	rows, err := db.DB.Query(sql)
	if err != nil {
		tx.AddError(err)
		return tx
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		tx.AddError(err)
		return tx
	}

	destValue := reflect.Indirect(reflect.ValueOf(dest))
	destType := destValue.Type()

	isSlice := destValue.Kind() == reflect.Slice

	var elemType reflect.Type
	if isSlice {
		elemType = destType.Elem()
	} else {
		elemType = destType
	}

	// Map columns to struct fields
	// We need to find the struct field for each column
	// Use schema to map column name to struct field
	colToField := make(map[string]string)
	for _, field := range schema.Fields {
		colToField[field.Name] = field.StructFieldName
	}

	// Handle Tags as well (they are in schema.Fields)

	for rows.Next() {
		// Create a new instance of the element
		// elemType is usually *Struct or Struct
		// If elemType is *Struct, we need to create Struct then take Addr

		var elem reflect.Value
		var scanElem reflect.Value // The struct we scan into

		if elemType.Kind() == reflect.Ptr {
			// elemType is *Struct
			scanElem = reflect.New(elemType.Elem()).Elem() // Struct
			elem = scanElem.Addr()                         // *Struct
		} else {
			// elemType is Struct
			scanElem = reflect.New(elemType).Elem()
			elem = scanElem
		}

		scanArgs := make([]interface{}, len(columns))
		for i, colName := range columns {
			fieldName, ok := colToField[colName]
			if ok {
				// We scan into scanElem (the struct value)
				f := scanElem.FieldByName(fieldName)
				if f.IsValid() {
					scanArgs[i] = f.Addr().Interface()
				} else {
					var ignore interface{}
					scanArgs[i] = &ignore
				}
			} else {
				// Column not in struct, ignore
				var ignore interface{}
				scanArgs[i] = &ignore
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			tx.AddError(err)
			return tx
		}

		if isSlice {
			destValue.Set(reflect.Append(destValue, elem))
		} else {
			destValue.Set(elem)
			break // Only fetch one if not slice
		}
	}

	return tx
}

func (db *DB) First(dest interface{}) *DB {
	tx := db.Limit(1).Find(dest)
	return tx
}
