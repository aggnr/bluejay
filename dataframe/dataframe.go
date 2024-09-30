package dataframe

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"strconv"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"db" // Import the db package
)

type DataFrame struct {
	Name       string
	StructType reflect.Type
	Data       map[int]interface{}
	Index      *db.BPlusTree // Use the BPlusTree from db package
	mutex      sync.RWMutex
}

func NewDataFrame() *DataFrame {
	return &DataFrame{
		Data:  make(map[int]interface{}),
		Index: db.NewBPlusTree(), // Initialize the BPlusTree
	}
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
	for id, row := range df.Data {
		if count >= rows {
			break
		}
		result = append(result, row.(map[string]interface{}))
		count++
	}
	return result, nil
}

func (df *DataFrame) Display(n ...int) error {
	rows, err := df.Head(n...)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		columns = append(columns, col)
	}
	fmt.Println(strings.Join(columns, "\t"))
	for _, row := range rows {
		values := make([]string, len(columns))
		for i, col := range columns {
			values[i] = fmt.Sprintf("%v", row[col])
		}
		fmt.Println(strings.Join(values, "\t"))
	}
	return nil
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

func (df *DataFrame) Join(other *DataFrame, joinType string, keys []string) (*DataFrame, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	other.mutex.RLock()
	defer other.mutex.RUnlock()

	result := NewDataFrame()
	result.Name = df.Name + "_" + other.Name + "_join"

	switch joinType {
	case "inner":
		for id, row := range df.Data {
			if otherRow, found := other.Data[id]; found {
				newRow := mergeRows(row.(map[string]interface{}), otherRow.(map[string]interface{}), df.Name, other.Name)
				result.InsertRow(id, newRow)
			}
		}
	case "left":
		for id, row := range df.Data {
			newRow := mergeRows(row.(map[string]interface{}), other.Data[id].(map[string]interface{}), df.Name, other.Name)
			result.InsertRow(id, newRow)
		}
	case "right":
		for id, row := range other.Data {
			newRow := mergeRows(df.Data[id].(map[string]interface{}), row.(map[string]interface{}), df.Name, other.Name)
			result.InsertRow(id, newRow)
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
			newRow := mergeRows(df.Data[id].(map[string]interface{}), other.Data[id].(map[string]interface{}), df.Name, other.Name)
			result.InsertRow(id, newRow)
		}
	default:
		return nil, fmt.Errorf("unsupported join type: %s", joinType)
	}

	return result, nil
}

func mergeRows(row1, row2 map[string]interface{}, name1, name2 string) map[string]interface{} {
	newRow := make(map[string]interface{})
	for k, v := range row1 {
		newRow[name1+"."+k] = v
	}
	for k, v := range row2 {
		newRow[name2+"."+k] = v
	}
	return newRow
}

// ReadJSONFromString takes a JSON string and a pointer to a struct, populates the struct with the JSON data.
func ReadJSONFromString(jsonData string, v interface{}) (*DataFrame, error) {
	// Unmarshal the JSON data into the struct
	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Ensure v is a slice of structs
	vSlice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(v).Elem()), 0, 0)
	vSlice = reflect.Append(vSlice, reflect.ValueOf(v).Elem())

	// Create a new DataFrame with the populated slice of structs
	return NewDataFrame(vSlice.Interface())
}

// ReadJSONFromFile takes a JSON file path and a pointer to a struct, reads the JSON data from the file.
func ReadJSONFromFile(jsonFilePath string, v interface{}) (*DataFrame, error) {
	file, err := os.Open(jsonFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	buffer := make([]byte, fileSize)

	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}

	jsonData := string(buffer)

	// Unmarshal the JSON data into the struct
	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Ensure v is a slice of structs
	vSlice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(v).Elem()), 0, 0)
	vSlice = reflect.Append(vSlice, reflect.ValueOf(v).Elem())

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
func (df *DataFrame) ToCSV(csvFilePath string, v interface{}) error {
	// Open the CSV file for writing
	file, err := os.Create(csvFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Get the table name and columns
	tableName := reflect.TypeOf(v).Elem().Name()
	rows, err := df.QueryRows("SELECT * FROM " + tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Write the header row
	if err := writer.Write(columns); err != nil {
		return err
	}

	// Write the data rows
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		record := make([]string, len(columns))
		for i, val := range values {
			if val != nil {
				switch v := val.(type) {
				case int64:
					record[i] = strconv.FormatInt(v, 10)
				case float64:
					record[i] = strconv.FormatFloat(v, 'f', -1, 64)
				case bool:
					record[i] = strconv.FormatBool(v)
				case time.Time:
					record[i] = v.Format(time.RFC3339)
				case []byte:
					record[i] = string(v)
				case string:
					record[i] = v
				default:
					record[i] = fmt.Sprintf("%v", v)
				}
			} else {
				record[i] = ""
			}
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}