package main

import (
	"log"
	"os"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// JSON data to be written to the file
	jsonData := `[
		{"Name": "John", "Age": 30, "Salary": 50000.50, "IsMarried": true},
		{"Name": "Jane", "Age": 25, "Salary": 60000.75, "IsMarried": false}
	]`

	// Write JSON data to a file
	file, err := os.Create("people.json")
	if err != nil {
		log.Fatalf("Error creating JSON file: %v", err)
	}
	defer file.Close()

	if _, err := file.WriteString(jsonData); err != nil {
		log.Fatalf("Error writing JSON to file: %v", err)
	}

	// Define the struct to hold the JSON data
	type Person struct {
		Name      string  `json:"name"`
		Age       int     `json:"age"`
		Salary    float64 `json:"salary"`
		IsMarried bool    `json:"is_married"`
	}

	var people []Person

	// Read JSON from file into a DataFrame
	df, err := dataframe.ReadJSONFromFile("people.json", &people)
	if err != nil {
		log.Fatalf("Error reading JSON from file: %v", err)
	}

	// Display the DataFrame
	if err := df.Display(); err != nil {
		log.Fatalf("Error displaying DataFrame: %v", err)
	}
}