package goframe

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestReadJSON(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	jsonStr := `{
		"name": "John",
		"age": 30,
		"salary": 50000.50,
		"is_married": true
	}`
	var person Person

	gf, err := ReadJSONFromString(jsonStr, &person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer gf.Close()

	// Insert the data into the SQLite table using the Insert method
	values := []interface{}{person.Name, person.Age, person.Salary, person.IsMarried}
	if err := gf.Insert(&person, values); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the struct fields
	expectedName := "John"
	expectedAge := 30
	expectedSalary := 50000.50
	expectedIsMarried := true

	if person.Name != expectedName {
		t.Errorf("Expected name %v, got %v", expectedName, person.Name)
	}
	if person.Age != expectedAge {
		t.Errorf("Expected age %v, got %v", expectedAge, person.Age)
	}
	if person.Salary != expectedSalary {
		t.Errorf("Expected salary %v, got %v", expectedSalary, person.Salary)
	}
	if person.IsMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v, got %v", expectedIsMarried, person.IsMarried)
	}

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool

	row := gf.Query("SELECT Name, Age, Salary, IsMarried FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if name != expectedName {
		t.Errorf("Expected name %v in database, got %v", expectedName, name)
	}
	if age != expectedAge {
		t.Errorf("Expected age %v in database, got %v", expectedAge, age)
	}
	if salary != expectedSalary {
		t.Errorf("Expected salary %v in database, got %v", expectedSalary, salary)
	}
	if isMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v in database, got %v", expectedIsMarried, isMarried)
	}
}

func TestReadJSONFromFile(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	// Create a temporary JSON file
	jsonContent := `{
        "name": "John",
        "age": 30,
        "salary": 50000.50,
        "is_married": true
    }`
	tmpFile, err := os.CreateTemp("", "test.json")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(jsonContent); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tmpFile.Close()

	var person Person

	gf, err := ReadJSONFromFile(tmpFile.Name(), &person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer gf.Close()

	// Insert the data into the SQLite table using the Insert method
	values := []interface{}{person.Name, person.Age, person.Salary, person.IsMarried}
	if err := gf.Insert(&person, values); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the struct fields
	expectedName := "John"
	expectedAge := 30
	expectedSalary := 50000.50
	expectedIsMarried := true

	if person.Name != expectedName {
		t.Errorf("Expected name %v, got %v", expectedName, person.Name)
	}
	if person.Age != expectedAge {
		t.Errorf("Expected age %v, got %v", expectedAge, person.Age)
	}
	if person.Salary != expectedSalary {
		t.Errorf("Expected salary %v, got %v", expectedSalary, person.Salary)
	}
	if person.IsMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v, got %v", expectedIsMarried, person.IsMarried)
	}

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool

	row := gf.Query("SELECT Name, Age, Salary, IsMarried FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if name != expectedName {
		t.Errorf("Expected name %v in database, got %v", expectedName, name)
	}
	if age != expectedAge {
		t.Errorf("Expected age %v in database, got %v", expectedAge, age)
	}
	if salary != expectedSalary {
		t.Errorf("Expected salary %v in database, got %v", expectedSalary, salary)
	}
	if isMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v in database, got %v", expectedIsMarried, isMarried)
	}

}

func TestReadCSVFromFile(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	// Create a temporary CSV file
	csvContent := `name,age,salary,is_married
John,30,50000.50,true`
	tmpFile, err := os.CreateTemp("", "test.csv")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tmpFile.Close()

	var person Person

	gf, err := ReadCSVFromFile(tmpFile.Name(), &person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer gf.Close()

	// Insert the data into the SQLite table using the Insert method
	values := []interface{}{person.Name, person.Age, person.Salary, person.IsMarried}
	if err := gf.Insert(&person, values); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the struct fields
	expectedName := "John"
	expectedAge := 30
	expectedSalary := 50000.50
	expectedIsMarried := true

	if person.Name != expectedName {
		t.Errorf("Expected name %v, got %v", expectedName, person.Name)
	}
	if person.Age != expectedAge {
		t.Errorf("Expected age %v, got %v", expectedAge, person.Age)
	}
	if person.Salary != expectedSalary {
		t.Errorf("Expected salary %v, got %v", expectedSalary, person.Salary)
	}
	if person.IsMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v, got %v", expectedIsMarried, person.IsMarried)
	}

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool
	row := gf.Query("SELECT Name, Age, Salary, IsMarried FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if name != expectedName {
		t.Errorf("Expected name %v in database, got %v", expectedName, name)
	}
	if age != expectedAge {
		t.Errorf("Expected age %v in database, got %v", expectedAge, age)
	}
	if salary != expectedSalary {
		t.Errorf("Expected salary %v in database, got %v", expectedSalary, salary)
	}
	if isMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v in database, got %v", expectedIsMarried, isMarried)
	}
}

func TestReadCSVFromString(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	// CSV data as a string
	csvData := `name,age,salary,is_married
John,30,50000.50,true`

	var person Person

	gf, err := ReadCSVFromString(csvData, &person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer gf.Close()

	// Insert the data into the SQLite table using the Insert method
	values := []interface{}{person.Name, person.Age, person.Salary, person.IsMarried}
	if err := gf.Insert(&person, values); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the struct fields
	expectedName := "John"
	expectedAge := 30
	expectedSalary := 50000.50
	expectedIsMarried := true

	if person.Name != expectedName {
		t.Errorf("Expected name %v, got %v", expectedName, person.Name)
	}
	if person.Age != expectedAge {
		t.Errorf("Expected age %v, got %v", expectedAge, person.Age)
	}
	if person.Salary != expectedSalary {
		t.Errorf("Expected salary %v, got %v", expectedSalary, person.Salary)
	}
	if person.IsMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v, got %v", expectedIsMarried, person.IsMarried)
	}

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool
	row := gf.Query("SELECT Name, Age, Salary, IsMarried FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if name != expectedName {
		t.Errorf("Expected name %v in database, got %v", expectedName, name)
	}
	if age != expectedAge {
		t.Errorf("Expected age %v in database, got %v", expectedAge, age)
	}
	if salary != expectedSalary {
		t.Errorf("Expected salary %v in database, got %v", expectedSalary, salary)
	}
	if isMarried != expectedIsMarried {
		t.Errorf("Expected is_married %v in database, got %v", expectedIsMarried, isMarried)
	}

}

func TestToCSV(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	// Create a sample struct and populate it with data
	person := Person{
		Name:      "John",
		Age:       30,
		Salary:    50000.50,
		IsMarried: true,
	}

	// Create a DataFrame
	df, err := populateStructAndSaveToDB(&person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert the data into the SQLite table using the Insert method
	values := []interface{}{person.Name, person.Age, person.Salary, person.IsMarried}
	if err := df.Insert(&person, values); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	defer df.Close()

	// Create a temporary CSV file
	tmpFile, err := os.CreateTemp("", "test.csv")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write the DataFrame to the CSV file
	if err := df.ToCSV(tmpFile.Name(), &person); err != nil { // Pass the person struct here
		t.Fatalf("Expected no error, got %v", err)
	}

	// Read the CSV file and verify its contents
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the header row
	expectedHeaders := []string{"Name", "Age", "Salary", "IsMarried"}
	if !reflect.DeepEqual(records[0], expectedHeaders) {
		t.Errorf("Expected headers %v, got %v", expectedHeaders, records[0])
	}

	// Verify the data row
	expectedData := []string{"John", "30", "50000.5", "true"}
	if !reflect.DeepEqual(records[1], expectedData) {
		t.Errorf("Expected data %v, got %v", expectedData, records[1])
	}
}

func TestPerformanceToCSV(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	// Create a sample struct and populate it with data
	person := Person{
		Name:      "John",
		Age:       30,
		Salary:    50000.50,
		IsMarried: true,
	}

	// Create a DataFrame and populate it with a million rows
	df, err := populateStructAndSaveToDB(&person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer df.Close()

	// Insert a million rows
	for i := 0; i < 1000000; i++ {
		if _, err := df.DB.Exec("INSERT INTO Person (Name, Age, Salary, IsMarried) VALUES (?, ?, ?, ?)", person.Name, person.Age, person.Salary, person.IsMarried); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	var count int
	if err := df.Query("select count(*) from Person").Scan(&count); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	fmt.Println(count)

	// Create a temporary CSV file
	tmpFile, err := os.CreateTemp("", "test.csv")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Measure CPU, memory, and time
	startTime := time.Now()
	var memStatsStart, memStatsEnd runtime.MemStats
	runtime.ReadMemStats(&memStatsStart)
	startCPU := runtime.NumCPU()

	// Write the DataFrame to the CSV file
	if err := df.ToCSV(tmpFile.Name(), &person); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	endTime := time.Now()
	runtime.ReadMemStats(&memStatsEnd)
	endCPU := runtime.NumCPU()

	// Calculate the metrics
	timeTaken := endTime.Sub(startTime)
	cpuUsed := endCPU - startCPU
	memUsed := memStatsEnd.Alloc - memStatsStart.Alloc

	// Print the performance stats
	t.Logf("Time taken: %v\n", timeTaken)
	t.Logf("CPU used: %d\n", cpuUsed)
	t.Logf("Memory used: %d bytes\n", memUsed)

	// Read the CSV file and verify its contents
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the header row
	expectedHeaders := []string{"Name", "Age", "Salary", "IsMarried"}
	if !reflect.DeepEqual(records[0], expectedHeaders) {
		t.Errorf("Expected headers %v, got %v", expectedHeaders, records[0])
	}

	// Verify the number of data rows
	if len(records)-1 != 1000000 {
		t.Errorf("Expected 1000000 rows, got %d", len(records)-1)
	}
}
