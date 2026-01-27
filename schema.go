package teorm

import (
	"reflect"
	"strings"
	"time"
    "unicode"
)

type Tabler interface {
	TableName() string
}

type Stabler interface {
	StableName() string
}

type Schema struct {
	Name      string
    TableName string // Sub table name or normal table name
	ModelType reflect.Type
	Fields    []*Field
	Tags      []*Field // Fields that are tags
	Cols      []*Field // Fields that are normal columns
}

type Field struct {
	Name            string
	StructFieldName string
	Type            string
	Tag             string // The raw tag string
	IsTag           bool   // Is this a TDengine TAG?
	IsPrimaryKey    bool
}

// Parse parses a struct to a Schema
func Parse(dest interface{}) *Schema {
	modelType := reflect.ValueOf(dest).Type()
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	if modelType.Kind() == reflect.Slice {
		modelType = modelType.Elem()
	}

	schema := &Schema{
		Name:      ToSnakeCase(modelType.Name()),
		ModelType: modelType,
	}

	// Check interfaces for Table/Stable Name
	// Use the actual value if available and not zero, otherwise use a new instance
	var modelValue interface{}
	val := reflect.ValueOf(dest)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}
	
	// If it's a slice, we can't determine dynamic tablename from the slice itself easily for all items
	// But Parse is called per item in Create loop (Wait, no, Parse is called once in Create top level)
	// In Create, we loop.
	// But here Parse takes interface{}.
	
	// If dest is a valid struct instance (not zero value), we should use it to call TableName
	if val.Kind() == reflect.Struct {
		modelValue = val.Addr().Interface() // Get pointer to struct to satisfy interface receiver if pointer
		// Check if it satisfies Tabler
		if tabler, ok := modelValue.(Tabler); ok {
			schema.TableName = tabler.TableName()
		}
	} else {
		// Fallback to new instance
		modelValue = reflect.New(modelType).Interface()
		if tabler, ok := modelValue.(Tabler); ok {
			schema.TableName = tabler.TableName()
		}
	}
	
	// Stabler is usually static (type level), so new instance is fine, but let's check instance too just in case
	if stabler, ok := modelValue.(Stabler); ok {
        schema.Name = stabler.StableName()
    } else {
		// Try with new instance if the above failed (e.g. if modelValue was from dest but dest didn't impl Stabler on ptr?)
		// Actually reflect.New(modelType) returns *Struct.
		newValue := reflect.New(modelType).Interface()
		if stabler, ok := newValue.(Stabler); ok {
			schema.Name = stabler.StableName()
		}
	}

	for i := 0; i < modelType.NumField(); i++ {
		fieldStruct := modelType.Field(i)
		if !fieldStruct.IsExported() {
			continue
		}

		field := &Field{
			Name:            ToSnakeCase(fieldStruct.Name),
			StructFieldName: fieldStruct.Name,
		}

		// Parse tag
		tagSetting := ParseTagSetting(fieldStruct.Tag.Get("teorm"))
        
        if val, ok := tagSetting["COLUMN"]; ok {
            field.Name = val
        }
        
        if _, ok := tagSetting["TAG"]; ok {
            field.IsTag = true
        }

        if _, ok := tagSetting["PRIMARYKEY"]; ok {
            field.IsPrimaryKey = true
        }

        if val, ok := tagSetting["TYPE"]; ok {
            field.Type = val
        } else {
            field.Type = DataTypeOf(fieldStruct.Type)
        }

        if field.IsTag {
            schema.Tags = append(schema.Tags, field)
        } else {
            schema.Cols = append(schema.Cols, field)
        }
		schema.Fields = append(schema.Fields, field)
	}

	return schema
}

func DataTypeOf(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "BOOL"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INT"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INT UNSIGNED"
	case reflect.Uint64:
		return "BIGINT UNSIGNED"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.String:
		return "BINARY(64)" // Default size
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP"
		}
	}
	return "BINARY(64)"
}

func ParseTagSetting(str string) map[string]string {
	settings := map[string]string{}
	tags := strings.Split(str, ";")
	for _, value := range tags {
		v := strings.Split(value, ":")
		k := strings.TrimSpace(strings.ToUpper(v[0]))
		if len(v) >= 2 {
			settings[k] = strings.Join(v[1:], ":")
		} else {
			settings[k] = k
		}
	}
	return settings
}

func ToSnakeCase(str string) string {
	var matchFirstCap = unicode.IsUpper
    
    var sb strings.Builder
    for i, r := range str {
        if i > 0 && matchFirstCap(r) {
             if !matchFirstCap(rune(str[i-1])) {
                 sb.WriteRune('_')
             }
        }
        sb.WriteRune(unicode.ToLower(r))
    }
	return sb.String()
}
