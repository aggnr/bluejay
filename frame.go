package goframe

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

// ReadJSONFromString takes a JSON string and a pointer to a struct, populates the struct with the JSON data,
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

func ReadJSONFromString(jsonData string, v interface{}) (*sql.DB, error) {
	// Unmarshal the JSON data into the struct
	if err := json.Unmarshal([]byte(jsonData), v); err != nil {
		return nil, err
	}

	// Populate the struct and save to the database
	return populateStructAndSaveToDB(v)
}

// ReadJSONFromFile takes a JSON file path and a pointer to a struct, reads the JSON data from the file,
// populates the struct with the JSON data, saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - jsonFilePath: A string containing the path to the JSON file.
// - v: A pointer to the struct that will be populated with the JSON data.
//
// Returns:
// - A pointer to the in-memory SQLite database.
// - An error if any occurs during the process.
func ReadJSONFromFile(jsonFilePath string, v interface{}) (*sql.DB, error) {
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
	return ReadJSONFromString(jsonData, v)
}

// ReadCSVFromFile takes a CSV file path and a pointer to a struct, populates the struct with the CSV data,
// saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - csvFilePath: A string containing the path to the CSV file.
// - v: A pointer to the struct that will be populated with the CSV data.
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
//	csvContent := `name,age,salary,is_married,birth_date
//	John,30,50000.50,true,1990-01-01T00:00:00Z`
//	tmpFile, err := os.CreateTemp("", "test.csv")
//	if err != nil {
//	    log.Fatalf("Error: %v", err)
//	}
//	defer os.Remove(tmpFile.Name())
//
//	if _, err := tmpFile.WriteString(csvContent); err != nil {
//	    log.Fatalf("Error: %v", err)
//	}
//	tmpFile.Close()
//
//	var person Person
//	gf, err := ReadCSV(tmpFile.Name(), &person)
//	if err != nil {
//	    log.Fatalf("Error: %v", err)
//	}
//	defer gf.Close()
//
//	fmt.Printf("Name: %s\n", person.Name)
//	fmt.Printf("Age: %d\n", person.Age)
//	fmt.Printf("Salary: %.2f\n", person.Salary)
//	fmt.Printf("IsMarried: %t\n", person.IsMarried)
//	fmt.Printf("BirthDate: %s\n", person.BirthDate.Format(time.RFC3339))

func ReadCSVFromFile(csvFilePath string, v interface{}) (*sql.DB, error) {
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
// saves the data into a SQLite in-memory table, and returns a pointer to the database.
//
// Parameters:
// - csvData: A string containing the CSV data.
// - v: A pointer to the struct that will be populated with the CSV data.
//
// Returns:
// - A pointer to the in-memory SQLite database.
// - An error if any occurs during the process.
func ReadCSVFromString(csvData string, v interface{}) (*sql.DB, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Assuming the first row contains headers and the second row contains data
	if len(records) < 2 {
		return nil, fmt.Errorf("CSV data does not contain enough data")
	}

	headers := records[0]
	data := records[1]

	val := reflect.ValueOf(v).Elem()
	typ := val.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		for j, header := range headers {
			if field.Tag.Get("json") == header {
				fieldValue := val.Field(i)
				switch fieldValue.Kind() {
				case reflect.String:
					fieldValue.SetString(data[j])
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					intValue, _ := strconv.ParseInt(data[j], 10, 64)
					fieldValue.SetInt(intValue)
				case reflect.Float32, reflect.Float64:
					floatValue, _ := strconv.ParseFloat(data[j], 64)
					fieldValue.SetFloat(floatValue)
				case reflect.Bool:
					boolValue, _ := strconv.ParseBool(data[j])
					fieldValue.SetBool(boolValue)
				case reflect.Struct:
					if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
						timeValue, _ := time.Parse(time.RFC3339, data[j])
						fieldValue.Set(reflect.ValueOf(timeValue))
					}
				}
			}
		}
	}

	// Populate the struct and save to the database
	return populateStructAndSaveToDB(v)
}

// populateStructAndSaveToDB takes a pointer to a struct, creates a SQLite in-memory table,
// saves the data into the table, and returns a pointer to the database.
func populateStructAndSaveToDB(v interface{}) (*sql.DB, error) {
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
