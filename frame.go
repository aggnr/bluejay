package bluejay

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// NewDataFrame creates a new DataFrame instance and populates it with the provided data.
func NewDataFrame(data interface{}) (*DataFrame, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	df := &DataFrame{DB: db}

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
	//if v.Kind() != reflect.Slice {
	//	return fmt.Errorf("data must be a slice of structs")
	//}

	if v.Len() == 0 {
		return fmt.Errorf("data slice is empty")
	}

	elemType := v.Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("data must be a slice of structs")
	}

	df.StructType = elemType

	// Use CreateTable method to create the table
	if err := df.CreateTable(data); err != nil {
		return err
	}

	for i := 0; i < v.Len(); i++ {
		structVal := v.Index(i)
		var values []interface{}
		for j := 0; j < structVal.NumField(); j++ {
			values = append(values, structVal.Field(j).Interface())
		}
		if err := df.Insert(data, values); err != nil {
			return err
		}
	}

	return nil
}

// ReadJSONFromString takes a JSON string and a pointer to a struct, populates the struct with the JSON data,
// saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - jsonData: A string containing the JSON data.
// - v: A pointer to the struct that will be populated with the JSON data.
//
// Returns:
// - A DataFrame.
// - An error if any occurs during the process.
//

func ReadJSONFromString(jsonData string, v interface{}) (*DataFrame, error) {
	// Unmarshal the JSON data into the struct
	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Create a new DataFrame with the populated struct
	return NewDataFrame(v)
}

// ReadJSONFromFile takes a JSON file path and a pointer to a struct, reads the JSON data from the file,
// populates the struct with the JSON data, saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - jsonFilePath: A string containing the path to the JSON file.
// - v: A pointer to the struct that will be populated with the JSON data.
//
// Returns:
// - A DataFrame.
// - An error if any occurs during the process.
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

	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Ensure v is a slice of structs
	vSlice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(v).Elem()), 0, 0)
	vSlice = reflect.Append(vSlice, reflect.ValueOf(v).Elem())

	// Create a new DataFrame with the populated slice of structs
	return NewDataFrame(vSlice.Interface())
}

// ReadCSVFromFile takes a CSV file path and a pointer to a struct, populates the struct with the CSV data,
// saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - csvFilePath: A string containing the path to the CSV file.
// - v: A pointer to the struct that will be populated with the CSV data.
//
// Returns:
// - A DataFrame.
// - An error if any occurs during the process.
//

func ReadCSVFromFile(csvFilePath string, v interface{}) (*DataFrame, error) {
	file, err := os.Open(csvFilePath)
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

	csvData := string(buffer)
	return ReadCSVFromString(csvData, v)
}

// ReadCSVFromString takes a CSV string and a pointer to a struct, populates the struct with the CSV data,
func ReadCSVFromString(csvData string, v interface{}) (*DataFrame, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
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
				return ok && field.Tag.Get("json") == header
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

	return NewDataFrame(vSlice.Interface())
}

// ToCSV writes the contents of a DataFrame to a CSV file.
//
// Parameters:
// - csvFilePath: The path to the CSV file.
//
// Returns:
// - An error if any occurs during the process.
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
