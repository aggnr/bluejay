// This example demonstrates how to read a CSV string into a DataFrame.
//go:build ignoreme
// +build ignoreme

package main

import (
	"log"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Initialize the global database connection
	if err := dataframe.Init(); err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer dataframe.Close()

	csvString := `Name,Age,Salary,IsMarried
John,30,50000.50,true
,,60000.75,false`

	type Person struct {
		Name      string
		Age       int
		Salary    float64
		IsMarried bool
	}

	df,_ :=dataframe.ReadCSVFromString(csvString, &Person{})

	df.Display()
}
