// This example demonstrates how to use the Head method to get the top rows of a DataFrame.
//go:build ignoreme
// +build ignoreme

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aggnr/goframe"
)

func main() {
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

	df, err := goframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}
	defer df.Close()

	fmt.Println("DataFrame created successfully!")

	// Use the Head method to get the top 5 rows
	topRows, err := df.Head()
	if err != nil {
		log.Fatalf("Error getting top rows: %v", err)
	}

	fmt.Println("Top 5 rows:")
	for _, row := range topRows {
		fmt.Println(row)
	}

	// Use the Head method to get the top 3 rows
	top3Rows, err := df.Head(3)
	if err != nil {
		log.Fatalf("Error getting top 3 rows: %v", err)
	}

	fmt.Println("Top 3 rows:")
	for _, row := range top3Rows {
		fmt.Println(row)
	}
}
