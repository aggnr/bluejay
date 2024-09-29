package dataframe

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
	"math"
	"github.com/aggnr/bluejay/viz"
)

var GlobalDB *sql.DB

// Core DataFrame.
type DataFrame struct {
	Name       string
	StructType reflect.Type
	Data       interface{}
}

// Row represents a single row in the DataFrame.
type Row struct {
	row *sql.Row
}

// Rows represents multiple rows in the DataFrame.
type Rows struct {
	rows *sql.Rows
}

// Close closes the underlying sql.Rows.
func (r *Rows) Close() error {
	return r.rows.Close()
}

// Columns returns the column names.
func (r *Rows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Next prepares the next row for reading.
func (r *Rows) Next() bool {
	return r.rows.Next()
}

// Scan copies the columns in the current row into the values pointed at by dest.
func (r *Rows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

// InitDB initializes the global database connection.
func Init() error {
	var err error
	GlobalDB, err = sql.Open("sqlite3", "identifier.sqlite")
	if err != nil {
		return err
	}

	// Call CleanUp to delete all tables
	if err := CleanUp(); err != nil {
		return err
	}

	return nil
}

// CleanUp closes the database connection and deletes all tables.
func CleanUp() error {
	if GlobalDB == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	// Query to get all table names
	rows, err := GlobalDB.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return err
	}
	defer rows.Close()

	var dropQueries []string
	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		dropQueries = append(dropQueries, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}

	for _, query := range dropQueries {
		if _, err := GlobalDB.Exec(query); err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

// Close closes the underlying SQLite database connection.
func Close() error {
	if GlobalDB != nil {
		if err := CleanUp(); err != nil {
			return err
		}
		return GlobalDB.Close()
	}
	return nil
}

// getTableNameAndColumns returns the table name and columns for a given struct.
func getTableNameAndColumns(v interface{}) (string, []string) {
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return "", nil
	}

	tableName := typ.Name()
	var columns []string
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		columns = append(columns, field.Name)
	}
	return tableName, columns
}

// Query wraps the underlying sql.DB.QueryRow method and returns a DataFrame.Row.
func (df *DataFrame) Query(query string, args ...any) *Row {
	return &Row{row: GlobalDB.QueryRow(query, args...)}
}

// QueryRows wraps the underlying sql.DB.Query method and returns a DataFrame.Rows.
func (df *DataFrame) QueryRows(query string, args ...any) (*Rows, error) {
	rows, err := GlobalDB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}

// Insert method for DataFrame
func (df *DataFrame) Insert(v interface{}, values []interface{}) error {
	tableName, columns := getTableNameAndColumns(v)
	placeholders := make([]string, len(values))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	_, err := GlobalDB.Exec(query, values...)
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
	_, err := GlobalDB.Exec(query, args...)
	return err
}

// Delete method for DataFrame
func (df *DataFrame) Delete(v interface{}, condition string, conditionArgs []interface{}) error {
	tableName, _ := getTableNameAndColumns(v)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	_, err := GlobalDB.Exec(query, conditionArgs...)
	return err
}

// CreateTable method for DataFrame
func (df *DataFrame) CreateTable(v interface{}) error {
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

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldName := field.Name
		fieldType := "TEXT"

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

	_, err := GlobalDB.Exec(createTableQuery)
	return err
}

// Loc method to return one or more specified rows
func (df *DataFrame) Loc(indices ...int) ([]map[string]interface{}, error) {
	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s WHERE rowid IN (%s)", tableName, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(indices)), ","), "[]"))

	rows, err := GlobalDB.Query(query)
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
func (df *DataFrame) Head(n ...int) ([]map[string]interface{}, error) {
	rows := 5
	if len(n) > 0 {
		rows = n[0]
	}

	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, rows)

	resultRows, err := GlobalDB.Query(query)
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
func (df *DataFrame) Display(n ...int) error {
	rows := 5
	if len(n) > 0 {
		rows = n[0]
	}

	tableName := df.Name
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, rows)

	resultRows, err := GlobalDB.Query(query)
	if err != nil {
		return err
	}
	defer resultRows.Close()

	columns, err := resultRows.Columns()
	if err != nil {
		return err
	}

	// Print the column headers
	fmt.Println(strings.Join(columns, "\t"))

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
		for i := range columns {
			row[i] = fmt.Sprintf("%v", values[i])
		}

		fmt.Println(strings.Join(row, "\t"))
	}

	return nil
}

// Tail method returns the bottom n rows, defaulting to 5
func (df *DataFrame) Tail(n ...int) ([]map[string]interface{}, error) {
	rows := 5
	if len(n) > 0 {
		rows = n[0]
	}

	tableName := df.StructType.Name()
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY ROWID DESC LIMIT %d", tableName, rows)

	resultRows, err := GlobalDB.Query(query)
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

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result, nil
}

// isNumericType checks if a given column type is numeric.
func isNumericType(columnType string) bool {
	numericTypes := map[string]bool{
		"INTEGER": true,
		"REAL":    true,
		"FLOAT":   true,
		"DOUBLE":  true,
	}
	return numericTypes[columnType]
}

// Info method returns and prints details about the DataFrame
func (df *DataFrame) Info() error {
	columns, err := GlobalDB.Query(fmt.Sprintf("PRAGMA table_info(%s)", df.StructType.Name()))
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

	fmt.Println("Column Names:", columnNames)
	fmt.Println("Column Types:", columnTypes)

	rowCount := 0
	rowQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", df.StructType.Name())
	err = GlobalDB.QueryRow(rowQuery).Scan(&rowCount)
	if err != nil {
		return err
	}
	fmt.Println("Number of Rows:", rowCount)

	nullCounts := make(map[string]int)
	nonNullCounts := make(map[string]int)
	ranges := make(map[string][2]interface{})

	for i, col := range columnNames {
		nullQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", df.StructType.Name(), col)
		nonNullQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", df.StructType.Name(), col)
		var nullCount, nonNullCount int

		err = GlobalDB.QueryRow(nullQuery).Scan(&nullCount)
		if err != nil {
			return err
		}
		err = GlobalDB.QueryRow(nonNullQuery).Scan(&nonNullCount)
		if err != nil {
			return err
		}

		nullCounts[col] = nullCount
		nonNullCounts[col] = nonNullCount

		if isNumericType(columnTypes[i]) {
			rangeQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", col, col, df.StructType.Name())
			var min, max interface{}
			err = GlobalDB.QueryRow(rangeQuery).Scan(&min, &max)
			if err != nil {
				return err
			}
			ranges[col] = [2]interface{}{min, max}
		}
	}

	fmt.Println("Null Counts:", nullCounts)
	fmt.Println("Non-Null Counts:", nonNullCounts)
	fmt.Println("Ranges for Numeric Columns:", ranges)

	return nil
}

// CorrelationMatrix represents the correlation between two columns.
type CorrelationMatrix struct {
	Column1 string
	Column2 string
	Value   float64
}

// Correlation method calculates the Pearson correlation coefficient between each pair of numeric columns and returns a DataFrame
func (df *DataFrame) Corr() (*DataFrame, error) {
	columns, err := GlobalDB.Query(fmt.Sprintf("PRAGMA table_info(%s)", df.StructType.Name()))
	if err != nil {
		return nil, err
	}
	defer columns.Close()

	var columnNames []string
	var columnTypes []string
	for columns.Next() {
		var cid int
		var name, ctype string
		var notnull, dflt_value, pk interface{}
		if err := columns.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, name)
		columnTypes = append(columnTypes, ctype)
	}

	numericColumns := []string{}
	for i, ctype := range columnTypes {
		if isNumericType(ctype) {
			numericColumns = append(numericColumns, columnNames[i])
		}
	}

	var corrData []CorrelationMatrix

	means := make(map[string]float64)
	stdDevs := make(map[string]float64)
	for _, col := range numericColumns {
		query := fmt.Sprintf("SELECT AVG(%s) FROM %s", col, df.StructType.Name())
		var mean float64
		err := GlobalDB.QueryRow(query).Scan(&mean)
		if err != nil {
			return nil, err
		}
		means[col] = mean

		varianceQuery := fmt.Sprintf("SELECT AVG((%s - ?) * (%s - ?)) FROM %s", col, col, df.StructType.Name())
		var variance float64
		err = GlobalDB.QueryRow(varianceQuery, mean, mean).Scan(&variance)
		if err != nil {
			return nil, err
		}
		stdDevs[col] = math.Sqrt(variance)
	}

	for i, col1 := range numericColumns {
		for j, col2 := range numericColumns {
			if i == j {
				corrData = append(corrData, CorrelationMatrix{col1, col2, 1.0})
				continue
			}
			covQuery := fmt.Sprintf("SELECT AVG((%s - ?) * (%s - ?)) FROM %s", col1, col2, df.StructType.Name())
			var cov float64
			err := GlobalDB.QueryRow(covQuery, means[col1], means[col2]).Scan(&cov)
			if err != nil {
				return nil, err
			}
			corr := cov / (stdDevs[col1] * stdDevs[col2])
			corrData = append(corrData, CorrelationMatrix{col1, col2, corr})
		}
	}

	corrDF := &DataFrame{
		StructType: reflect.TypeOf(CorrelationMatrix{}),
		Data:       corrData,
	}

	return corrDF, nil
}

// Helper function to calculate the square root
func sqrt(x float64) float64 {
	return x * x
}

// ToMatrix converts the DataFrame to a 2D slice of float64 and returns the column names
func (df *DataFrame) ToMatrix() ([][]float64, []string) {
	corrData, ok := df.Data.([]CorrelationMatrix)
	if (!ok) {
		return nil, nil
	}

	// Create a map to store the indices of each column
	columnIndices := make(map[string]int)
	index := 0
	for _, row := range corrData {
		if _, exists := columnIndices[row.Column1]; !exists {
			columnIndices[row.Column1] = index
			index++
		}
		if _, exists := columnIndices[row.Column2]; !exists {
			columnIndices[row.Column2] = index
			index++
		}
	}

	// Initialize the matrix
	size := len(columnIndices)
	matrix := make([][]float64, size)
	for i := range matrix {
		matrix[i] = make([]float64, size)
	}

	// Fill the matrix with correlation values
	for _, row := range corrData {
		i := columnIndices[row.Column1]
		j := columnIndices[row.Column2]
		matrix[i][j] = row.Value
		matrix[j][i] = row.Value // Ensure symmetry
	}

	// Extract column names
	columns := make([]string, size)
	for col, idx := range columnIndices {
		columns[idx] = col
	}

	return matrix, columns
}

// Display prints the correlation matrix in a readable format
func (df *DataFrame) DisplayCorr() {
	// Assuming df.Data contains the correlation matrix data
	corrData, ok := df.Data.([]CorrelationMatrix)
	if !ok {
		fmt.Println("Invalid data format for correlation matrix")
		return
	}

	fmt.Println("Correlation Matrix:")
	for _, row := range corrData {
		fmt.Printf("%s-%s: %.2f\n", row.Column1, row.Column2, row.Value)
	}
}

// ShowPlot method to display a graph using the viz package
func (df *DataFrame) ShowPlot(xCol, yCol string, title string) error {
	query := fmt.Sprintf("SELECT %s, %s FROM %s", xCol, yCol, df.StructType.Name())
	rows, err := GlobalDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var dataChan chan [2]float64 = nil

	var xData, yData []float64
	for rows.Next() {
		var x, y float64
		if err := rows.Scan(&x, &y); err != nil {
			return err
		}
		xData = append(xData, x)
		yData = append(yData, y)
	}

	viz.ShowPlot(xData, yData, xCol, yCol, title, dataChan)

	return nil
}

// Join performs a join on two DataFrames based on the specified key and join type
func (df *DataFrame) Join(other *DataFrame, joinType string, keys []string) (*DataFrame, error) {
	validJoinTypes := map[string]bool{"inner": true, "outer": true, "left": true, "right": true}
	if !validJoinTypes[joinType] {
		return nil, fmt.Errorf("invalid join type: %s", joinType)
	}

	// Get the type of the data in both DataFrames
	dfType := df.StructType
	otherType := other.StructType

	// Create the SELECT clause with renamed columns
	var selectClauses []string
	for i := 0; i < dfType.NumField(); i++ {
		field := dfType.Field(i)
		selectClauses = append(selectClauses, fmt.Sprintf("%s.%s AS %s_%s", dfType.Name(), field.Name, dfType.Name(), field.Name))
	}
	for i := 0; i < otherType.NumField(); i++ {
		field := otherType.Field(i)
		selectClauses = append(selectClauses, fmt.Sprintf("%s.%s AS %s_%s", otherType.Name(), field.Name, otherType.Name(), field.Name))
	}

	// Construct the join query
	joinQuery := fmt.Sprintf("SELECT %s FROM %s %s JOIN %s ON ", strings.Join(selectClauses, ", "), dfType.Name(), strings.ToUpper(joinType), otherType.Name())
	joinConditions := []string{}
	for _, col := range keys {
		joinConditions = append(joinConditions, fmt.Sprintf("%s.%s = %s.%s", dfType.Name(), col, otherType.Name(), col))
	}
	joinQuery += strings.Join(joinConditions, " AND ")

	// Create a meaningful name for the joined table
	joinedTableName := fmt.Sprintf("%s_%s_joined", df.Name, other.Name)

	// Create the joined table
	createTableQuery := fmt.Sprintf("CREATE TABLE %s AS %s", joinedTableName, joinQuery)
	if _, err := GlobalDB.Exec(createTableQuery); err != nil {
		return nil, err
	}

	// Get the columns of the joined table
	columns, err := GlobalDB.Query(fmt.Sprintf("PRAGMA table_info(%s)", joinedTableName))
	if err != nil {
		return nil, err
	}
	defer columns.Close()

	// Create a new struct type based on the columns of the joined table
	var fields []reflect.StructField
	for columns.Next() {
		var cid int
		var name, ctype string
		var notnull, dflt_value, pk interface{}
		if err := columns.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err != nil {
			return nil, err
		}

		fieldType := reflect.TypeOf("")
		switch ctype {
		case "INTEGER":
			fieldType = reflect.TypeOf(int(0))
		case "REAL":
			fieldType = reflect.TypeOf(float64(0))
		case "BOOLEAN":
			fieldType = reflect.TypeOf(bool(false))
		case "DATETIME":
			fieldType = reflect.TypeOf(time.Time{})
		}

		fields = append(fields, reflect.StructField{
			Name: strings.Title(name),
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s"`, name)),
		})
	}
	combinedType := reflect.StructOf(fields)


	// Create the joined DataFrame
	joinedDF := &DataFrame{
		Name:       joinedTableName,
		StructType: combinedType,
		Data:       nil,
	}

	return joinedDF, nil
}