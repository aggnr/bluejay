package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Initialize the global database connection
	if err := dataframe.Init(); err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer dataframe.Close()

	type Person struct {
		ID        int
		Name      string
		Age       int
		Birthdate time.Time
	}

	people := []Person{
		{ID: 1, Name: "Alice", Age: 30, Birthdate: time.Now()},
		{ID: 2, Name: "Bob", Age: 25, Birthdate: time.Now()},
		{ID: 3, Name: "Charlie", Age: 35, Birthdate: time.Now()},
		{ID: 4, Name: "Diana", Age: 28, Birthdate: time.Now()},
		{ID: 5, Name: "Eve", Age: 22, Birthdate: time.Now()},
		{ID: 6, Name: "Frank", Age: 40, Birthdate: time.Now()},
	}

	df, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Use the Display method to print the top 5 rows
	fmt.Println("Top 5 rows:")
	df.Display()

	// Use the Display method to print the top 3 rows
	fmt.Println("Top 3 rows:")
	df.Display(3)
}