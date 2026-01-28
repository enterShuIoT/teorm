package teorm

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
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
			Schema   *Schema
			Elements []reflect.Value
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

			var val interface{}
			// If ptr, dereference for value
			if fVal.Kind() == reflect.Ptr {
				val = fVal.Elem().Interface()
			} else {
				val = fVal.Interface()
			}

			allColValues = append(allColValues, val)
		}
		colPlaceholders = append(colPlaceholders, colPlaceholdersStr)
	}

	// Construct SQL
	var sqlStr string
	//var args []interface{}

	if len(tagValues) > 0 {
		// Optimization: Inline TAG values to avoid parameter binding issues with TDengine
		var tagValStrs []string
		for _, tv := range tagValues {
			tagValStrs = append(tagValStrs, formatTagValue(tv))
		}

		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) USING %s TAGS (%s) VALUES %s",
			db.Statement.Table,
			strings.Join(colNames, ", "),
			schema.Name,
			strings.Join(tagValStrs, ", "),
			// strings.Join(colPlaceholders, ", "), // Don't use placeholders
			buildInlinedValues(elements, schema, colNames),
		)
		// args = append(args, allColValues...) // Don't use args
	} else {
		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			db.Statement.Table,
			strings.Join(colNames, ", "),
			// strings.Join(colPlaceholders, ", "),
			buildInlinedValues(elements, schema, colNames),
		)
		// args = append(args, allColValues...)
	}

	// FORCE PRINT SQL to Stderr for debugging
	fmt.Fprintf(os.Stderr, "[DEBUG] Executing SQL: %s\n", sqlStr)

	_, err := db.DB.Exec(sqlStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[DEBUG] Error executing SQL: %v\n", err)
		db.AddError(err)
	}
}

func buildInlinedValues(elements []reflect.Value, schema *Schema, colNames []string) string {
	var rowStrs []string
	for _, elem := range elements {
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		var valStrs []string
		for _, colName := range colNames {
			// Find field
			var fieldName string
			for _, f := range schema.Cols {
				if f.Name == colName {
					fieldName = f.StructFieldName
					break
				}
			}
			fVal := elem.FieldByName(fieldName)
			var val interface{}
			if fVal.Kind() == reflect.Ptr {
				if fVal.IsNil() {
					val = nil
				} else {
					val = fVal.Elem().Interface()
				}
			} else {
				val = fVal.Interface()
			}
			valStrs = append(valStrs, formatTagValue(val))
		}
		rowStrs = append(rowStrs, "("+strings.Join(valStrs, ", ")+")")
	}
	return strings.Join(rowStrs, ", ")
}

func formatTagValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
	case []byte:
		return fmt.Sprintf("'%s'", string(val))
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format(time.RFC3339Nano))
	case nil:
		return "NULL"
	default:
		// Handle pointer dereference
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				return "NULL"
			}
			return formatTagValue(rv.Elem().Interface())
		}
		return fmt.Sprintf("%v", val)
	}
}

func Explain(sqlStr string, args ...interface{}) string {
	for _, arg := range args {
		var val string
		switch v := arg.(type) {
		case string:
			val = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "\\'"))
		case []byte:
			val = fmt.Sprintf("'%s'", string(v))
		case time.Time:
			val = fmt.Sprintf("'%s'", v.UTC().Format(time.RFC3339Nano))
		case nil:
			val = "NULL"
		default:
			// Handle pointer dereference for printing if needed, though args usually have values
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Ptr {
				if rv.IsNil() {
					val = "NULL"
				} else {
					val = fmt.Sprintf("%v", rv.Elem().Interface())
				}
			} else {
				val = fmt.Sprintf("%v", v)
			}
		}
		sqlStr = strings.Replace(sqlStr, "?", val, 1)
	}
	return sqlStr
}

// Table specifies the table name
func (db *DB) Table(name string) *DB {
	tx := db.getInstance()
	tx.Statement.Table = name
	return tx
}
