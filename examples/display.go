// Description: This example demonstrates how to create a DataFrame from a slice of structs and display the top rows using the Display method.

//go:build ignoreme
// +build ignoreme

package main

import (
	"fmt"
	"time"

	"github.com/aggnr/cow"
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

	df, _ := cow.NewDataFrame(people)
	defer df.Close()

	// Use the Display method to print the top 5 rows
	fmt.Println("Top 5 rows:")
	df.Display()

	// Use the Display method to print the top 3 rows
	fmt.Println("Top 3 rows:")
	df.Display(3)
}
