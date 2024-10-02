package dataframe_row

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"io"
	"fmt"
	"strconv"
	"reflect"
	"strings"
	"sync"
	"time"
	"math"
	"github.com/aggnr/bluejay/db" // Import the db package
)

type DataFrame struct {
	Name       string
	StructType reflect.Type
	Data       map[int]interface{}
	Index      *db.BPlusTree // Use the BPlusTree from db package
	mutex      sync.RWMutex
}

func NewDataFrame(data interface{}) (*DataFrame, error) {
	df := &DataFrame{
		Data:  make(map[int]interface{}),
		Index: db.NewBPlusTree(), // Initialize the BPlusTree
	}

	if err := df.FromStructs(data); err != nil {
		return nil, err
	}

	return df, nil
}

// FromStructs creates a DataFrame from a slice of structs.
func (df *DataFrame) FromStructs(data interface{}) error {

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Len() == 0 {
		return fmt.Errorf("data slice is empty")
	}

	elemType := v.Type().Elem()

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("data must be a slice of structs")
	}

	df.StructType = elemType
	df.Name = elemType.Name()

	for i := 0; i < v.Len(); i++ {
		structVal := v.Index(i)
		values := make(map[string]interface{})
		for j := 0; j < structVal.NumField(); j++ {
			values[structVal.Type().Field(j).Name] = structVal.Field(j).Interface()
		}
		df.InsertRow(i, values)
	}

	return nil
}

func (df *DataFrame) InsertRow(id int, row interface{}) {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	df.Data[id] = row
	df.Index.Insert(id) // Insert the key into the BPlusTree
}

func (df *DataFrame) ReadRow(id int) (interface{}, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	found := df.Index.Search(id) // Search for the key in the BPlusTree
	if !found {
		return nil, fmt.Errorf("row with ID %d not found", id)
	}
	return df.Data[id], nil
}

func (df *DataFrame) UpdateRow(id int, newValues map[string]interface{}) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	found := df.Index.Search(id) // Search for the key in the BPlusTree
	if !found {
		return fmt.Errorf("row with ID %d not found", id)
	}
	df.Data[id] = newValues
	return nil
}

func (df *DataFrame) DeleteRow(id int) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	found := df.Index.Search(id) // Search for the key in the BPlusTree
	if !found {
		return fmt.Errorf("row with ID %d not found", id)
	}
	delete(df.Data, id)
	df.Index.Delete(id) // Delete the key from the BPlusTree
	return nil
}

func (df *DataFrame) Loc(indices ...int) ([]map[string]interface{}, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	var result []map[string]interface{}
	for _, id := range indices {
		found := df.Index.Search(id) // Search for the key in the BPlusTree
		if !found {
			return nil, fmt.Errorf("row with ID %d not found", id)
		}
		result = append(result, df.Data[id].(map[string]interface{}))
	}
	return result, nil
}

func (df *DataFrame) Head(n ...int) ([]map[string]interface{}, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	rows := 5
	if len(n) > 0 {
		rows = n[0]
	}
	var result []map[string]interface{}
	count := 0
	for id := range df.Data {
		if count >= rows {
			break
		}
		found := df.Index.Search(id) // Search for the key in the BPlusTree
		if !found {
			continue
		}
		result = append(result, df.Data[id].(map[string]interface{}))
		count++
	}
	return result, nil
}

func (df *DataFrame) Tail(n ...int) ([]map[string]interface{}, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	rows := 5
	if len(n) > 0 {
		rows = n[0]
	}
	var result []map[string]interface{}
	count := 0
	for id := len(df.Data) - 1; id >= 0; id-- {
		if count >= rows {
			break
		}
		found := df.Index.Search(id) // Search for the key in the BPlusTree
		if !found {
			continue
		}
		result = append(result, df.Data[id].(map[string]interface{}))
		count++
	}
	return result, nil
}

// Display prints the contents of the DataFrame to the console.
func (df *DataFrame) Display() error {
	if len(df.Data) == 0 {
		return fmt.Errorf("dataframe is empty")
	}

	// Print the header row
	firstRow := df.Data[0].(map[string]interface{})
	var headers []string
	for col := range firstRow {
		headers = append(headers, col)
	}
	fmt.Println(headers)

	// Print the data rows
	for _, row := range df.Data {
		rowMap := row.(map[string]interface{})
		var record []string
		for _, col := range headers {
			record = append(record, fmt.Sprintf("%v", rowMap[col]))
		}
		fmt.Println(record)
	}

	return nil
}

func (df *DataFrame) Info() error {
	df.mutex.RLock()
	defer df.mutex.RUnlock()

	fmt.Println("DataFrame Name:", df.Name)
	fmt.Println("Number of Rows:", len(df.Data))

	if len(df.Data) > 0 {
		firstRow := df.Data[0].(map[string]interface{})
		fmt.Println("Number of Columns:", len(firstRow))
		fmt.Println("Columns:")
		for col, val := range firstRow {
			fmt.Printf(" - %s: %s\n", col, reflect.TypeOf(val).String())
		}
	} else {
		fmt.Println("Number of Columns: 0")
	}

	return nil
}

func mergeStructs(row1, row2 interface{}, newStructType reflect.Type) reflect.Value {
	newRow := reflect.New(newStructType).Elem()
	row1Val := reflect.ValueOf(row1)
	row2Val := reflect.ValueOf(row2)

	for i := 0; i < newStructType.NumField(); i++ {
		field := newStructType.Field(i)
		if field.Anonymous {
			continue
		}

		var val reflect.Value
		fieldName := field.Name
		if strings.HasSuffix(fieldName, "_df") {
			fieldName = strings.TrimSuffix(fieldName, "_df")
		} else if strings.HasSuffix(fieldName, "_other") {
			fieldName = strings.TrimSuffix(fieldName, "_other")
		}

		if row1Val.Kind() == reflect.Map {
			val = row1Val.MapIndex(reflect.ValueOf(fieldName))
		} else if row1Val.Kind() == reflect.Struct {
			val = row1Val.FieldByName(fieldName)
		}

		if !val.IsValid() && row2Val.Kind() == reflect.Map {
			val = row2Val.MapIndex(reflect.ValueOf(fieldName))
		} else if !val.IsValid() && row2Val.Kind() == reflect.Struct {
			val = row2Val.FieldByName(fieldName)
		}

		if val.IsValid() {
			if val.Type().Kind() == reflect.Interface {
				val = val.Elem()
			}
			if val.Type().AssignableTo(field.Type) {
				newRow.Field(i).Set(val)
			} else {
				newRow.Field(i).Set(reflect.Zero(field.Type))
			}
		} else {
			newRow.Field(i).Set(reflect.Zero(field.Type))
		}
	}
	return newRow
}

func (df *DataFrame) Join(other *DataFrame, joinType string, keys []string) (*DataFrame, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	other.mutex.RLock()
	defer other.mutex.RUnlock()

	// Convert struct fields to slices and rename duplicates
	dfFields := make([]reflect.StructField, df.StructType.NumField())
	fieldNames := make(map[string]bool)
	for i := 0; i < df.StructType.NumField(); i++ {
		field := df.StructType.Field(i)
		if fieldNames[field.Name] {
			field.Name += "_df"
		}
		fieldNames[field.Name] = true
		dfFields[i] = field
	}

	otherFields := make([]reflect.StructField, other.StructType.NumField())
	for i := 0; i < other.StructType.NumField(); i++ {
		field := other.StructType.Field(i)
		if fieldNames[field.Name] {
			field.Name += "_other"
		}
		fieldNames[field.Name] = true
		otherFields[i] = field
	}

	// Define a new struct type that combines the fields of the input structs
	newStructType := reflect.StructOf(append(dfFields, otherFields...))

	// Create a new slice of the combined struct type
	newSlice := reflect.MakeSlice(reflect.SliceOf(newStructType), 0, 0)

	// Create a new DataFrame with the combined struct type
	result := &DataFrame{
		Name:       df.Name + "_" + other.Name + "_join",
		StructType: newStructType,
		Data:       make(map[int]interface{}),
		Index:      db.NewBPlusTree(),
	}

	switch joinType {
	case "inner":
		for id, row := range df.Data {
			if otherRow, found := other.Data[id]; found {
				newRow := mergeStructs(row, otherRow, newStructType)
				newSlice = reflect.Append(newSlice, newRow)
			}
		}
	case "left":
		for id, row := range df.Data {
			newRow := mergeStructs(row, other.Data[id], newStructType)
			newSlice = reflect.Append(newSlice, newRow)
		}
	case "right":
		for id, row := range other.Data {
			newRow := mergeStructs(df.Data[id], row, newStructType)
			newSlice = reflect.Append(newSlice, newRow)
		}
	case "outer":
		allKeys := make(map[int]struct{})
		for id := range df.Data {
			allKeys[id] = struct{}{}
		}
		for id := range other.Data {
			allKeys[id] = struct{}{}
		}
		for id := range allKeys {
			newRow := mergeStructs(df.Data[id], other.Data[id], newStructType)
			newSlice = reflect.Append(newSlice, newRow)
		}
	default:
		return nil, fmt.Errorf("unsupported join type: %s", joinType)
	}

	// Populate the result DataFrame with the new slice
	if err := result.FromStructs(newSlice.Interface()); err != nil {
		return nil, err
	}

	return result, nil
}

// ReadJSONFromString takes a JSON string and a pointer to a slice of structs, populates the slice with the JSON data.
func ReadJSONFromString(jsonData string, v interface{}) (*DataFrame, error) {
	// Unmarshal the JSON data into the slice of structs
	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Ensure v is a slice of structs
	vSlice := reflect.ValueOf(v).Elem()

	// Create a new DataFrame with the populated slice of structs
	return NewDataFrame(vSlice.Interface())
}

// ReadJSONFromFile takes a JSON file path and a pointer to a struct, populates the struct with the JSON data.
func ReadJSONFromFile(jsonFilePath string, v interface{}) (*DataFrame, error) {
	// Read the JSON file
	file, err := os.Open(jsonFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the file content
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Unmarshal the JSON data into the struct
	if err := json.Unmarshal(content, v); err != nil {
		return nil, err
	}

	/// Ensure v is a slice of structs
	vSlice := reflect.ValueOf(v).Elem()

	// Create a new DataFrame with the populated slice of structs
	return NewDataFrame(vSlice.Interface())
}

// ReadCSVFromFile takes a CSV file path and a pointer to a struct, populates the struct with the CSV data.
func ReadCSVFromFile(csvFilePath string, v interface{}) (*DataFrame, error) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	return readCSV(reader, v)
}

// ReadCSVFromString takes a CSV string and a pointer to a struct, populates the struct with the CSV data.
func ReadCSVFromString(csvData string, v interface{}) (*DataFrame, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	return readCSV(reader, v)
}

// readCSV is a helper function that reads CSV data using the provided reader and populates the struct.
func readCSV(reader *csv.Reader, v interface{}) (*DataFrame, error) {
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("CSV data does not contain enough data")
	}

	headers := records[0]
	data := records[1:]

	vSlice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(v).Elem()), 0, len(data))

	for _, record := range data {
		elem := reflect.New(reflect.TypeOf(v).Elem()).Elem()
		for i, header := range headers {
			field := elem.FieldByNameFunc(func(name string) bool {
				field, ok := reflect.TypeOf(v).Elem().FieldByName(name)
				return ok && (strings.EqualFold(field.Tag.Get("json"), header) || strings.EqualFold(name, header))
			})

			if field.IsValid() {
				switch field.Kind() {
				case reflect.String:
					field.SetString(record[i])
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					if val, err := strconv.ParseInt(record[i], 10, 64); err == nil {
						field.SetInt(val)
					}
				case reflect.Float32, reflect.Float64:
					if val, err := strconv.ParseFloat(record[i], 64); err == nil {
						field.SetFloat(val)
					}
				case reflect.Bool:
					if val, err := strconv.ParseBool(record[i]); err == nil {
						field.SetBool(val)
					}
				case reflect.Struct:
					if field.Type() == reflect.TypeOf(time.Time{}) {
						if val, err := time.Parse(time.RFC3339, record[i]); err == nil {
							field.Set(reflect.ValueOf(val))
						}
					}
				}
			}
		}
		vSlice = reflect.Append(vSlice, elem)
	}

	df, err := NewDataFrame(vSlice.Interface())
	if err != nil {
		return nil, err
	}

	return df, nil
}

// ToCSV writes the contents of a DataFrame to a CSV file.
func (df *DataFrame) ToCSV(csvFilePath string) error {
	// Open the CSV file for writing
	file, err := os.Create(csvFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header row
	if len(df.Data) == 0 {
		return fmt.Errorf("dataframe is empty")
	}

	firstRow := df.Data[0].(map[string]interface{})
	var headers []string
	for col := range firstRow {
		headers = append(headers, col)
	}
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write the data rows
	for _, row := range df.Data {
		rowMap := row.(map[string]interface{})
		var record []string
		for _, col := range headers {
			record = append(record, fmt.Sprintf("%v", rowMap[col]))
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// Corr calculates the correlation matrix for the numerical columns in the DataFrame.
func (df *DataFrame) Corr() (*DataFrame, error) {
	if len(df.Data) == 0 {
		return nil, errors.New("dataframe is empty")
	}

	// Extract numerical columns
	numCols := []string{}
	for col, val := range df.Data[0].(map[string]interface{}) {
		if isNumeric(val) {
			numCols = append(numCols, col)
		}
	}

	if len(numCols) == 0 {
		return nil, errors.New("no numerical columns found")
	}

	// Initialize correlation matrix
	corrMatrix := make([][]float64, len(numCols))
	for i := range corrMatrix {
		corrMatrix[i] = make([]float64, len(numCols))
	}

	// Calculate correlations
	for i, col1 := range numCols {
		for j, col2 := range numCols {
			if i == j {
				corrMatrix[i][j] = 1.0
			} else if i < j {
				corr, err := calculateCorrelation(df, col1, col2)
				if err != nil {
					return nil, err
				}
				corrMatrix[i][j] = corr
				corrMatrix[j][i] = corr
			}
		}
	}

	// Convert correlation matrix to DataFrame
	corrData := make(map[int]interface{})
	for i, row := range corrMatrix {
		rowMap := map[string]interface{}{}
		for j, val := range row {
			rowMap[numCols[j]] = val
		}
		corrData[i] = rowMap
	}

	return &DataFrame{Data: corrData}, nil
}

// Helper function to check if a value is numeric
func isNumeric(val interface{}) bool {
	switch val.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}

// Helper function to calculate the correlation between two columns
func calculateCorrelation(df *DataFrame, col1, col2 string) (float64, error) {
	var sumX, sumY, sumXY, sumX2, sumY2 float64
	var n float64

	for _, row := range df.Data {
		rowMap := row.(map[string]interface{})
		x, ok1 := ToFloat64(rowMap[col1])
		y, ok2 := ToFloat64(rowMap[col2])
		if !ok1 || !ok2 {
			return 0, errors.New("non-numeric value encountered")
		}

		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
		sumY2 += y * y
		n++
	}

	numerator := n*sumXY - sumX*sumY
	denominator := math.Sqrt((n*sumX2 - sumX*sumX) * (n*sumY2 - sumY*sumY))
	if denominator == 0 {
		return 0, errors.New("division by zero in correlation calculation")
	}

	return numerator / denominator, nil
}

// Helper function to convert a value to float64
func ToFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// ToMatrix converts the DataFrame to a 2D slice of float64 and returns the column names.
func (df *DataFrame) ToMatrix() ([][]float64, []string, error) {
	if len(df.Data) == 0 {
		return nil, nil, errors.New("dataframe is empty")
	}

	// Extract column names
	firstRow := df.Data[0].(map[string]interface{})
	var columns []string
	for col := range firstRow {
		columns = append(columns, col)
	}

	// Initialize the matrix
	matrix := make([][]float64, len(df.Data))
	for i := range matrix {
		matrix[i] = make([]float64, len(columns))
	}

	// Fill the matrix with data
	for i, row := range df.Data {
		rowMap := row.(map[string]interface{})
		for j, col := range columns {
			val, ok := rowMap[col].(float64)
			if !ok {
				return nil, nil, errors.New("non-numeric value encountered")
			}
			matrix[i][j] = val
		}
	}

	return matrix, columns, nil
}