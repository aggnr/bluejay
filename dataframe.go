package goframe

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// DataFrame is a wrapper type over the SQLite database connection.
type DataFrame struct {
	DB *sql.DB
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

// getTableNameAndColumns infers the table name and column names from the struct type
func getTableNameAndColumns(v interface{}) (string, []string) {
	typ := reflect.TypeOf(v).Elem()
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
