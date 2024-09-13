package goframe

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// DataFrame is a wrapper type over the SQLite database connection.
type DataFrame struct {
	DB         *sql.DB
	StructType reflect.Type
}

// Row is a wrapper type over the sql.Row.
type Row struct {
	row *sql.Row
}

// Rows is a wrapper type over the sql.Rows.
type Rows struct {
	rows *sql.Rows
}

// Close closes the underlying SQLite database connection.
func (df *DataFrame) Close() error {
	if df.DB != nil {
		return df.DB.Close()
	}
	return nil
}

// Query wraps the underlying sql.DB.QueryRow method and returns a DataFrame.Row.
func (df *DataFrame) Query(query string, args ...any) *Row {
	return &Row{row: df.DB.QueryRow(query, args...)}
}

// QueryRows wraps the underlying sql.DB.Query method and returns a DataFrame.Rows.
func (df *DataFrame) QueryRows(query string, args ...any) (*Rows, error) {
	rows, err := df.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}

// Scan wraps the underlying sql.Row.Scan method.
func (r *Row) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

// Columns wraps the underlying sql.Rows.Columns method.
func (r *Rows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Next wraps the underlying sql.Rows.Next method.
func (r *Rows) Next() bool {
	return r.rows.Next()
}

// Scan wraps the underlying sql.Rows.Scan method.
func (r *Rows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Close wraps the underlying sql.Rows.Close method.
func (r *Rows) Close() error {
	return r.rows.Close()
}

func getTableNameAndColumns(v interface{}) (string, []string) {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		panic("data must be a slice of structs")
	}

	tableName := typ.Name()
	columns := make([]string, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		columns[i] = typ.Field(i).Name
	}
	return tableName, columns
}

// Insert method for DataFrame
func (df *DataFrame) Insert(v interface{}, values []interface{}) error {
	tableName, columns := getTableNameAndColumns(v)
	placeholders := make([]string, len(values))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	_, err := df.DB.Exec(query, values...)
	return err
}

// Update method for DataFrame
func (df *DataFrame) Update(v interface{}, values []interface{}, condition string, conditionArgs []interface{}) error {
	tableName, columns := getTableNameAndColumns(v)
	setClauses := make([]string, len(columns))
	for i, col := range columns {
		setClauses[i] = fmt.Sprintf("%s = ?", col)
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(setClauses, ", "), condition)
	args := append(values, conditionArgs...)
	_, err := df.DB.Exec(query, args...)
	return err
}

// Delete method for DataFrame
func (df *DataFrame) Delete(v interface{}, condition string, conditionArgs []interface{}) error {
	tableName, _ := getTableNameAndColumns(v)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	_, err := df.DB.Exec(query, conditionArgs...)
	return err
}

func (df *DataFrame) CreateTable(v interface{}) error {
	// Get the type of the struct
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("data must be a slice of structs")
	}

	tableName := typ.Name()
	createTableQuery := fmt.Sprintf("CREATE TABLE %s (", tableName)

	// Generate the columns and their types
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
				fieldType = "DATETIME"
			}
		}

		createTableQuery += fmt.Sprintf("%s %s,", fieldName, fieldType)
	}
	createTableQuery = strings.TrimSuffix(createTableQuery, ",") + ");"

	// Execute the create table query
	_, err := df.DB.Exec(createTableQuery)
	return err
}

// Loc method to return one or more specified rows
func (df *DataFrame) Loc(indices ...int) ([]map[string]interface{}, error) {
	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s WHERE rowid IN (%s)", tableName, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(indices)), ","), "[]"))

	rows, err := df.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		result = append(result, row)
	}

	return result, nil
}
