//go:build ignoreme
// +build ignoreme

package main

import (
	"fmt"
	"github.com/aggnr/goframe"
	"log"
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

	df, err := goframe.ReadJSONFromString(jsonString, &people)
	if err != nil {
		log.Fatalf("Error loading JSON string: %v", err)
	}

	defer df.Close()

	fmt.Println("DataFrame created successfully from JSON string!")
}
