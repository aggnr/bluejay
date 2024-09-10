package goframe

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// FromJSON takes a JSON string and a pointer to a struct, populates the struct with the JSON data,
// saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - jsonData: A string containing the JSON data.
// - v: A pointer to the struct that will be populated with the JSON data.
//
// Returns:
// - A pointer to the in-memory SQLite database.
// - An error if any occurs during the process.
//
// Example usage:
//
//	type Person struct {
//	    Name      string    `json:"name"`
//	    Age       int       `json:"age"`
//	    Salary    float64   `json:"salary"`
//	    IsMarried bool      `json:"is_married"`
//	    BirthDate time.Time `json:"birth_date"`
//	}
//
//	jsonStr := `{
//	    "name": "John",
//	    "age": 30,
//	    "salary": 50000.50,
//	    "is_married": true,
//	    "birth_date": "1990-01-01T00:00:00Z"
//	}`
//
//	var person Person
//	gf, err := FromJSON(jsonStr, &person)
//	if err != nil {
//	    log.Fatalf("Error: %v", err)
//	}
//	defer gf.Close()
func FromJSON(jsonData string, v interface{}) (*sql.DB, error) {
	// Unmarshal the JSON data into the struct
	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Create an in-memory SQLite database
	gf, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	// Get the type of the struct
	val := reflect.ValueOf(v).Elem()
	typ := val.Type()

	// Create a table with the same name as the struct
	tableName := typ.Name()
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (", tableName)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldName := field.Name
		fieldType := "TEXT" // Default to TEXT

		// Determine the SQLite field type based on the Go field type
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldType = "INTEGER"
		case reflect.Float32, reflect.Float64:
			fieldType = "REAL"
		case reflect.Bool:
			fieldType = "BOOLEAN"
		case reflect.String:
			fieldType = "TEXT"
		default:
			if field.Type == reflect.TypeOf(time.Time{}) {
				tag := field.Tag.Get("json")
				switch tag {
				case "date":
					fieldType = "DATE"
				case "time":
					fieldType = "TIME"
				case "datetime":
					fieldType = "DATETIME"
				case "timestamp":
					fieldType = "TIMESTAMP"
				default:
					fieldType = "TEXT"
				}
			}
		}

		createTableQuery += fmt.Sprintf("%s %s,", fieldName, fieldType)
	}
	createTableQuery = createTableQuery[:len(createTableQuery)-1] + ");"

	// Execute the create table query
	if _, err := gf.Exec(createTableQuery); err != nil {
		return nil, err
	}

	// Insert the data into the table
	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES (", tableName)
	values := make([]interface{}, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		insertQuery += "?,"
		values[i] = val.Field(i).Interface()
	}
	insertQuery = insertQuery[:len(insertQuery)-1] + ");"

	// Execute the insert query
	if _, err := gf.Exec(insertQuery, values...); err != nil {
		return nil, err
	}

	return gf, nil
}
