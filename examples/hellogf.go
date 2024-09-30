package main

import (
	"fmt"
	"log"

	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Initialize the global database connection
	if err := dataframe.Init(); err != nil {
		log.Fatalf("Error initializing database: %v", err)
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

	// Create a new DataFrame and populate it with the slice of structs
	df := dataframe.NewDataFrame()
	if err := df.FromStructs(people); err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Display the DataFrame
	if err := df.Display(); err != nil {
		log.Fatalf("Error displaying DataFrame: %v", err)
	}

	fmt.Println("DataFrame created and displayed successfully!")
}