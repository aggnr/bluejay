package bluejay

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
	"encoding/csv"
	"strconv"
)

// Core DataFrame.
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
// Example usage can be found [here](https://github.com/aggnr/bluejay/blob/main/examples/loc.go).
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

// Head method returns the top n rows, defaulting to 5
// Example usage can be found [here](https://github.com/aggnr/bluejay/blob/main/examples/head.go).
func (df *DataFrame) Head(n ...int) ([]map[string]interface{}, error) {
	rows := 5 // default number of rows
	if len(n) > 0 {
		rows = n[0]
	}

	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, rows)

	resultRows, err := df.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer resultRows.Close()

	columns, err := resultRows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for resultRows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := resultRows.Scan(valuePtrs...); err != nil {
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

// Display method prints the top n rows in tabular format, defaulting to 5
// Example usage can be found [here](https://github.com/aggnr/bluejay/blob/main/examples/display.go).
func (df *DataFrame) Display(n ...int) error {
	rows := 5 // default number of rows
	if len(n) > 0 {
		rows = n[0]
	}

	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, rows)

	resultRows, err := df.DB.Query(query)
	if err != nil {
		return err
	}
	defer resultRows.Close()

	columns, err := resultRows.Columns()
	if err != nil {
		return err
	}

	// Print column names
	fmt.Println(strings.Join(columns, "\t"))

	// Print rows
	for resultRows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := resultRows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make([]string, len(columns))
		for i, _ := range columns {
			row[i] = fmt.Sprintf("%v", values[i])
		}

		fmt.Println(strings.Join(row, "\t"))
	}

	return nil
}

// Tail method returns the bottom n rows, defaulting to 5
// Example usage can be found [here](https://github.com/aggnr/bluejay/blob/main/examples/tail.go).
func (df *DataFrame) Tail(n ...int) ([]map[string]interface{}, error) {
	rows := 5 // default number of rows
	if len(n) > 0 {
		rows = n[0]
	}

	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY ROWID DESC LIMIT %d", tableName, rows)

	resultRows, err := df.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer resultRows.Close()

	columns, err := resultRows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for resultRows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := resultRows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		result = append(result, row)
	}

	// Reverse the result to maintain the original order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result, nil
}

// Info method returns and prints details about the DataFrame
// Example usage can be found [here](https://github.com/aggnr/bluejay/blob/main/examples/info.go).
func (df *DataFrame) Info() error {
	// Get column names and types
	columns, err := df.DB.Query(fmt.Sprintf("PRAGMA table_info(%s)", df.StructType.Name()))
	if err != nil {
		return err
	}
	defer columns.Close()

	var columnNames []string
	var columnTypes []string
	for columns.Next() {
		var cid int
		var name, ctype string
		var notnull, dflt_value, pk interface{}
		if err := columns.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err != nil {
			return err
		}
		columnNames = append(columnNames, name)
		columnTypes = append(columnTypes, ctype)
	}

	// Print column names and types
	fmt.Println("Column Names:", columnNames)
	fmt.Println("Column Types:", columnTypes)

	// Get number of rows
	rowCount := 0
	rowQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", df.StructType.Name())
	err = df.DB.QueryRow(rowQuery).Scan(&rowCount)
	if err != nil {
		return err
	}
	fmt.Println("Number of Rows:", rowCount)

	// Get number of null and non-null values, and range for numeric columns
	nullCounts := make(map[string]int)
	nonNullCounts := make(map[string]int)
	ranges := make(map[string][2]interface{})

	for i, col := range columnNames {
		nullQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", df.StructType.Name(), col)
		nonNullQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", df.StructType.Name(), col)
		var nullCount, nonNullCount int

		err = df.DB.QueryRow(nullQuery).Scan(&nullCount)
		if err != nil {
			return err
		}
		err = df.DB.QueryRow(nonNullQuery).Scan(&nonNullCount)
		if err != nil {
			return err
		}

		nullCounts[col] = nullCount
		nonNullCounts[col] = nonNullCount

		// Check if column is numeric and get range
		if isNumericType(columnTypes[i]) {
			minQuery := fmt.Sprintf("SELECT MIN(%s) FROM %s", col, df.StructType.Name())
			maxQuery := fmt.Sprintf("SELECT MAX(%s) FROM %s", col, df.StructType.Name())
			var min, max interface{}

			err = df.DB.QueryRow(minQuery).Scan(&min)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			err = df.DB.QueryRow(maxQuery).Scan(&max)
			if err != nil && err != sql.ErrNoRows {
				return err
			}

			ranges[col] = [2]interface{}{min, max}
		}
	}

	// Print null and non-null counts
	fmt.Println("Null Counts:", nullCounts)
	fmt.Println("Non-Null Counts:", nonNullCounts)
	fmt.Println("Ranges for Numeric Columns:", ranges)

	return nil
}

// Helper function to check if a column type is numeric
func isNumericType(ctype string) bool {
	numericTypes := []string{"INTEGER", "REAL", "FLOAT", "DOUBLE"}
	for _, t := range numericTypes {
		if ctype == t {
			return true
		}
	}
	return false
}