package teorm

import (
	"fmt"
	"strings"
)

func (db *DB) AutoMigrate(dst interface{}) error {
	schema := Parse(dst)
	
	// Build CREATE STABLE statement
	// CREATE STABLE IF NOT EXISTS name (cols) TAGS (tags)
	
	var colDefs []string
	for _, field := range schema.Cols {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", field.Name, field.Type))
	}
	
	var tagDefs []string
	for _, field := range schema.Tags {
		tagDefs = append(tagDefs, fmt.Sprintf("%s %s", field.Name, field.Type))
	}
	
	sql := fmt.Sprintf("CREATE STABLE IF NOT EXISTS %s (%s) TAGS (%s)", 
		schema.Name, 
		strings.Join(colDefs, ", "), 
		strings.Join(tagDefs, ", "))
		
	if _, err := db.DB.Exec(sql); err != nil {
		return fmt.Errorf("failed to create stable %s: %w", schema.Name, err)
	}
	
	return nil
}
