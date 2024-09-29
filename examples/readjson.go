// This example demonstrates how to read a JSON string into a DataFrame.
//go:build ignoreme
// +build ignoreme

package main

import (
	"log"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Initialize the global database connection
	if err := dataframe.Init(); err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer dataframe.Close()

	jsonString := `[
        {"Name": "John", "Age": 30, "Salary": 50000.50, "IsMarried": true},
        {"Name": "Jane", "Age": 25, "Salary": 60000.75, "IsMarried": false}
    ]`

	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	var people []Person

	df, _ := dataframe.ReadJSONFromString(jsonString, &people)

	df.Display()
}
