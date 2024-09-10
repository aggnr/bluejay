package main

import (
	"testing"
	"time"
)

func TestFromJSON(t *testing.T) {
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

	gf, err := FromJSON(jsonStr, &person)
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
