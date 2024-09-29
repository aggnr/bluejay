package main

import (
	"fmt"
	"log"

	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Initialize the global database connection
	if err := dataframe.Init(); err != nil {
		log.Fatalf("Error initializing %v", err)
	}
	defer dataframe.Close()

	type Person struct {
		Name      string
		Age       int
		Salary    float64
		IsMarried bool
	}

	people := []Person{
		{"John", 30, 50000.50, true},
		{"Jane", 25, 60000.75, false},
	}

	_, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	fmt.Println("DataFrame created successfully!")
}