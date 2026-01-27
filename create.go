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
	// OLD: extracted from first element for all.
	// NEW: now we use dynamic grouping below.
	
	// 2. Collect Values for all rows
	for _, elem := range elements {
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		
		// For dynamic columns, we need to check if we can batch insert with same columns
		// TDengine batch insert requires all rows to have same column structure?
		// "INSERT INTO ... VALUES (row1), (row2)" - standard SQL requires consistent columns.
		// If rows have different non-nil columns, we MUST split them into different INSERT statements.
		// For now, let's assume we simply insert NULL for nil pointers in batch mode to keep performance high.
		// Wait, user requirement is "prevent value set to 0".
		// If we insert NULL, it overwrites old value with NULL.
		// If user wants PARTIAL UPDATE (keep old value), TDengine doesn't support this via simple INSERT easily if row exists.
		// But if the requirement is just "don't write 0", writing NULL is often what is expected for "no value".
		
		// However, if user explicitly wants "don't touch this column", then we CANNOT include it in INSERT statement.
		// This means we can't batch heterogeneous rows together in one INSERT statement.
		
		// To strictly follow "partial insert" (only insert non-nil fields), we must:
		// 1. Group by "Set of Non-Nil Columns".
		// 2. Execute batch for each group.
		
		// Let's implement this "Sub-Grouping by Column Signature".
	}
	
	// Refactored batchInsert implementation below to support Dynamic Column Grouping
	
	// Map signature (bitmask or string of col names) -> list of elements
	type ColumnGroup struct {
		Signature string
		ColNames  []string
		Elements  []reflect.Value
	}
	colGroups := make(map[string]*ColumnGroup)
	
	for _, elem := range elements {
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		
		var activeColNames []string
		var sigBuilder strings.Builder
		
		for _, field := range schema.Cols {
			fVal := elem.FieldByName(field.StructFieldName)
			
			// Check if nil
			isNil := false
			if fVal.Kind() == reflect.Ptr && fVal.IsNil() {
				isNil = true
			}
			
			if !isNil {
				activeColNames = append(activeColNames, field.Name)
				sigBuilder.WriteString(field.Name)
				sigBuilder.WriteString(",")
			}
		}
		
		sig := sigBuilder.String()
		if _, ok := colGroups[sig]; !ok {
			colGroups[sig] = &ColumnGroup{
				Signature: sig,
				ColNames:  activeColNames,
				Elements:  []reflect.Value{},
			}
		}
		colGroups[sig].Elements = append(colGroups[sig].Elements, elem)
	}
	
	// Execute INSERT for each Column Group
	for _, grp := range colGroups {
		if len(grp.ColNames) == 0 {
			// No columns to insert? Maybe only tags?
			// If only tags, we can insert.
		}
		db.executeGroupBatchInsert(grp.Elements, schema, grp.ColNames, tagValues, tagPlaceholders)
	}
}

func (db *DB) executeGroupBatchInsert(elements []reflect.Value, schema *Schema, colNames []string, tagValues []interface{}, tagPlaceholders []string) {
	var colPlaceholders []string
	var allColValues []interface{}
	
	// Prepare placeholder for one row
	singleRowPlaceholders := make([]string, len(colNames))
	for i := range singleRowPlaceholders {
		singleRowPlaceholders[i] = "?"
	}
	colPlaceholdersStr := "(" + strings.Join(singleRowPlaceholders, ", ") + ")"
	
	for _, elem := range elements {
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		
		// Extract values for active columns only
		for _, colName := range colNames {
			// Find field by colName (Need reverse lookup or iterate schema)
			// Optimization: Store mapping in Schema? For now iterate.
			var fieldName string
			for _, f := range schema.Cols {
				if f.Name == colName {
					fieldName = f.StructFieldName
					break
				}
			}
			
			fVal := elem.FieldByName(fieldName)
			val := fVal.Interface()
			// If ptr, dereference for value (unless driver handles *int)
			// driver-go handles *int as value or NULL if nil. 
			// But here we already filtered nil, so it's non-nil pointer.
			allColValues = append(allColValues, val)
		}
		colPlaceholders = append(colPlaceholders, colPlaceholdersStr)
	}
	
	// Construct SQL
	var sqlStr string
	var args []interface{}

	if len(tagValues) > 0 {
		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) USING %s TAGS (%s) VALUES %s",
			db.Statement.Table,
			strings.Join(colNames, ", "),
			schema.Name,
			strings.Join(tagPlaceholders, ", "),
			strings.Join(colPlaceholders, ", "),
		)
		args = append(args, tagValues...)
		args = append(args, allColValues...)
	} else {
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
