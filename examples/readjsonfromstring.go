package main

import (
	"log"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {
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

	// Read JSON into a DataFrame
	df, err := dataframe.ReadJSONFromString(jsonString, &people)
	if err != nil {
		log.Fatalf("Error reading JSON: %v", err)
	}

	// Display the DataFrame
	if err := df.Display(); err != nil {
		log.Fatalf("Error displaying DataFrame: %v", err)
	}
}