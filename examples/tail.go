package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aggnr/bluejay/dataframe"
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

	df, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Use the Tail method to get the bottom 5 rows
	bottomRows, err := df.Tail()
	if err != nil {
		log.Fatalf("Error getting bottom rows: %v", err)
	}

	fmt.Println("Bottom 5 rows:")
	for _, row := range bottomRows {
		fmt.Println(row)
	}

	// Use the Tail method to get the bottom 3 rows
	bottom3Rows, err := df.Tail(3)
	if err != nil {
		log.Fatalf("Error getting bottom 3 rows: %v", err)
	}

	fmt.Println("Bottom 3 rows:")
	for _, row := range bottom3Rows {
		fmt.Println(row)
	}
}