package teorm

import (
	"fmt"
	"reflect"
	"strings"
)

// Create inserts value into database
func (db *DB) Create(value interface{}) *DB {
	tx := db.getInstance()
	
	// Handle Slice
	destValue := reflect.ValueOf(value)
	if destValue.Kind() == reflect.Ptr {
		destValue = destValue.Elem()
	}
	
	if destValue.Kind() == reflect.Slice {
		// Group elements by TableName
		type BatchGroup struct {
			Schema    *Schema
			Elements  []reflect.Value
		}
		groups := make(map[string]*BatchGroup)
		
		for i := 0; i < destValue.Len(); i++ {
			elem := destValue.Index(i)
			
			// Parse Schema & TableName
			var elemInterface interface{}
			if elem.Kind() == reflect.Struct && elem.CanAddr() {
				elemInterface = elem.Addr().Interface()
			} else {
				elemInterface = elem.Interface()
			}
			
			schema := Parse(elemInterface)
			tableName := tx.Statement.Table
			
			if tableName == "" {
				if schema.TableName != "" {
					tableName = schema.TableName
				} else if len(schema.Tags) == 0 {
					tableName = schema.Name
				}
			}
			
			if tableName == "" {
				tx.AddError(fmt.Errorf("table name is required at index %d", i))
				return tx
			}
			
			// Add to group
			if _, ok := groups[tableName]; !ok {
				groups[tableName] = &BatchGroup{
					Schema:   schema,
					Elements: []reflect.Value{},
				}
			}
			groups[tableName].Elements = append(groups[tableName].Elements, elem)
		}
		
		// Execute batch insert per group
		for tableName, group := range groups {
			groupTx := tx.getInstance()
			groupTx.Statement.Table = tableName
			groupTx.batchInsert(group.Elements, group.Schema)
			if groupTx.Error != nil {
				tx.AddError(groupTx.Error)
				// Continue or break? Usually continue for other tables, but error is recorded
			}
		}
		
		return tx
	}
	
    schema := Parse(value)
    
	// Determine Table Name
	if tx.Statement.Table == "" {
		if schema.TableName != "" {
			tx.Statement.Table = schema.TableName
		} else if len(schema.Tags) == 0 {
			tx.Statement.Table = schema.Name
		}
	}
	
	if tx.Statement.Table == "" {
		tx.AddError(fmt.Errorf("table name is required, use db.Table('name') or implement Tabler interface"))
		return tx
	}
	
	tx.insert(destValue, schema)
	return tx
}

func (db *DB) insert(val reflect.Value, schema *Schema) {
	// Re-use batchInsert for single element
	db.batchInsert([]reflect.Value{val}, schema)
}

func (db *DB) batchInsert(elements []reflect.Value, schema *Schema) {
	if len(elements) == 0 {
		return
	}

	var tagValues []interface{}
	var tagPlaceholders []string
	var colNames []string
	var colPlaceholders []string // "(?, ?, ...)" for one row
	var allColValues []interface{}

	// We assume all elements in this batch group have the same Schema structure (Tags, Cols)
	// and same Tag Values (if they belong to the same subtable, tags must be same!)
	
	firstElem := elements[0]
	if firstElem.Kind() == reflect.Ptr {
		firstElem = firstElem.Elem()
	}

	// 1. Prepare Metadata (Columns & Tags) from first element
	
	// Extract Tags
	for _, field := range schema.Tags {
		fVal := firstElem.FieldByName(field.StructFieldName)
		tagValues = append(tagValues, fVal.Interface())
		tagPlaceholders = append(tagPlaceholders, "?")
	}

	// Extract Column Names
	var singleRowPlaceholders []string
	for _, field := range schema.Cols {
		colNames = append(colNames, field.Name)
		singleRowPlaceholders = append(singleRowPlaceholders, "?")
	}
	colPlaceholdersStr := "(" + strings.Join(singleRowPlaceholders, ", ") + ")"

	// 2. Collect Values for all rows
	for _, elem := range elements {
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		for _, field := range schema.Cols {
			fVal := elem.FieldByName(field.StructFieldName)
			allColValues = append(allColValues, fVal.Interface())
		}
		colPlaceholders = append(colPlaceholders, colPlaceholdersStr)
	}

	// 3. Construct SQL
	// INSERT INTO table (cols) 
	// USING stable TAGS (tags) 
	// VALUES (row1), (row2), ...
	
	var sqlStr string
	var args []interface{}

	if len(tagValues) > 0 {
		// With Tags (Auto Create SubTable)
		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) USING %s TAGS (%s) VALUES %s",
			db.Statement.Table,
			strings.Join(colNames, ", "),
			schema.Name, // Stable name
			strings.Join(tagPlaceholders, ", "),
			strings.Join(colPlaceholders, ", "),
		)
		args = append(args, tagValues...)
		args = append(args, allColValues...)
	} else {
		// Normal Insert
		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			db.Statement.Table,
			strings.Join(colNames, ", "),
			strings.Join(colPlaceholders, ", "),
		)
		args = append(args, allColValues...)
	}

	_, err := db.DB.Exec(sqlStr, args...)
	if err != nil {
		db.AddError(err)
	}
}

// Table specifies the table name
func (db *DB) Table(name string) *DB {
	tx := db.getInstance()
	tx.Statement.Table = name
	return tx
}
