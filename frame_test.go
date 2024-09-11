package goframe

import (
	"os"
	"testing"
	"time"
)

func TestReadJSON(t *testing.T) {
	type Person struct {
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		Salary    float64   `json:"salary"`
		IsMarried bool      `json:"is_married"`
		BirthDate time.Time `json:"birth_date"`
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
	var birthDateStr string
	row := gf.QueryRow("SELECT Name, Age, Salary, IsMarried, BirthDate FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried, &birthDateStr); err != nil {
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
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		Salary    float64   `json:"salary"`
		IsMarried bool      `json:"is_married"`
		BirthDate time.Time `json:"birth_date"`
	}

	// Create a temporary JSON file
	jsonContent := `{
        "name": "John",
        "age": 30,
        "salary": 50000.50,
        "is_married": true,
        "birth_date": "1990-01-01T00:00:00Z"
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
	expectedBirthDate, _ := time.Parse(time.RFC3339, "1990-01-01T00:00:00Z")

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
	if !person.BirthDate.Equal(expectedBirthDate) {
		t.Errorf("Expected birth_date %v, got %v", expectedBirthDate, person.BirthDate)
	}

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool
	var birthDateStr string
	row := gf.QueryRow("SELECT Name, Age, Salary, IsMarried, BirthDate FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried, &birthDateStr); err != nil {
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
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		Salary    float64   `json:"salary"`
		IsMarried bool      `json:"is_married"`
		BirthDate time.Time `json:"birth_date"`
	}

	// Create a temporary CSV file
	csvContent := `name,age,salary,is_married,birth_date
John,30,50000.50,true,1990-01-01T00:00:00Z`
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
	var birthDateStr string
	row := gf.QueryRow("SELECT Name, Age, Salary, IsMarried, BirthDate FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried, &birthDateStr); err != nil {
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
		Name      string    `json:"name"`
		Age       int       `json:"age"`
		Salary    float64   `json:"salary"`
		IsMarried bool      `json:"is_married"`
		BirthDate time.Time `json:"birth_date"`
	}

	// CSV data as a string
	csvData := `name,age,salary,is_married,birth_date
John,30,50000.50,true,1990-01-01T00:00:00Z`

	var person Person

	gf, err := ReadCSVFromString(csvData, &person)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer gf.Close()

	// Verify the struct fields
	expectedName := "John"
	expectedAge := 30
	expectedSalary := 50000.50
	expectedIsMarried := true
	expectedBirthDate, _ := time.Parse(time.RFC3339, "1990-01-01T00:00:00Z")

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
	if !person.BirthDate.Equal(expectedBirthDate) {
		t.Errorf("Expected birth_date %v, got %v", expectedBirthDate, person.BirthDate)
	}

	// Verify the data in the SQLite table
	var name string
	var age int
	var salary float64
	var isMarried bool
	var birthDateStr string
	row := gf.QueryRow("SELECT Name, Age, Salary, IsMarried, BirthDate FROM Person")
	if err := row.Scan(&name, &age, &salary, &isMarried, &birthDateStr); err != nil {
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
