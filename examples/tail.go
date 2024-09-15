// This example demonstrates how to use the Tail method to get the bottom rows of a DataFrame.
//go:build ignoreme
// +build ignoreme

package main

import (
	"fmt"
	"time"

	"github.com/aggnr/bluejay"
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

	df, _ := bluejay.NewDataFrame(people)

	defer df.Close()

	// Use the Tail method to get the bottom 5 rows
	bottomRows, _ := df.Tail()

	fmt.Println("Bottom 5 rows:")
	for _, row := range bottomRows {
		fmt.Println(row)
	}

	// Use the Tail method to get the bottom 3 rows
	bottom3Rows, err := df.Tail(3)

	fmt.Println("Bottom 3 rows:")
	for _, row := range bottom3Rows {
		fmt.Println(row)
	}
}
