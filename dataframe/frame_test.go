package dataframe

import (
	"encoding/csv"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestNewDataFrame(t *testing.T) {
	type Person struct {
		Name      string
		Age       int
		Salary    float64
		IsMarried bool
	}

	// Create a slice of Person structs
	people := []Person{
		{"John", 30, 50000.50, true},
		{"Jane", 25, 60000.75, false},
	}

	// Create a new DataFrame
	df, err := NewDataFrame(people)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer df.DB.Close()

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool

	rows, err := df.DB.Query("SELECT Name, Age, Salary, IsMarried FROM Person")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		if err := rows.Scan(&name, &age, &salary, &isMarried); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if name != people[i].Name {
			t.Errorf("Expected name %v, got %v", people[i].Name, name)
		}
		if age != people[i].Age {
			t.Errorf("Expected age %v, got %v", people[i].Age, age)
		}
		if salary != people[i].Salary {
			t.Errorf("Expected salary %v, got %v", people[i].Salary, salary)
		}
		if isMarried != people[i].IsMarried {
			t.Errorf("Expected is_married %v, got %v", people[i].IsMarried, isMarried)
		}
		i++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestReadJSON(t *testing.T) {
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	jsonStr := `[
        {
            "name": "John",
            "age": 30,
            "salary": 50000.50,
            "is_married": true
        },
        {
            "name": "Jane",
            "age": 25,
            "salary": 60000.75,
            "is_married": false
        }
    ]`
	var people []Person

	gf, err := ReadJSONFromString(jsonStr, &people)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer gf.Close()

	// Verify the struct fields
	expectedPeople := []Person{
		{"John", 30, 50000.50, true},
		{"Jane", 25, 60000.75, false},
	}

	for i, person := range people {
		if person.Name != expectedPeople[i].Name {
			t.Errorf("Expected name %v, got %v", expectedPeople[i].Name, person.Name)
		}
		if person.Age != expectedPeople[i].Age {
			t.Errorf("Expected age %v, got %v", expectedPeople[i].Age, person.Age)
		}
		if person.Salary != expectedPeople[i].Salary {
			t.Errorf("Expected salary %v, got %v", expectedPeople[i].Salary, person.Salary)
		}
		if person.IsMarried != expectedPeople[i].IsMarried {
			t.Errorf("Expected is_married %v, got %v", expectedPeople[i].IsMarried, person.IsMarried)
		}
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

	// Create a slice of Person structs
	people := []Person{person}

	// Create a new DataFrame
	df, err := NewDataFrame(people)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer df.DB.Close()

	// Create a temporary CSV file
	tmpFile, err := os.CreateTemp("", "test.csv")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write the DataFrame to the CSV file
	if err := df.ToCSV(tmpFile.Name(), &person); err != nil {
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

	n := 10
	// Create a slice of Person structs
	people := make([]Person, n)
	for i := range people {
		people[i] = person
	}

	// Create a new DataFrame
	df, err := NewDataFrame(people)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer df.DB.Close()

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
	if len(records)-1 != n {
		t.Errorf("Expected %d rows, got %d", n, len(records)-1)
	}
}
