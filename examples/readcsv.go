// This example demonstrates how to read a CSV string into a DataFrame.
//go:build ignoreme
// +build ignoreme

package main

import (
	"fmt"
	"github.com/aggnr/goframe"
	"log"
)

func main() {
	csvString := `Name,Age,Salary,IsMarried
	John,30,50000.50,true
	Jane,25,60000.75,false`

	type Person struct {
		Name      string
		Age       int
		Salary    float64
		IsMarried bool
	}

	df, err := goframe.ReadCSVFromString(csvString, &Person{})
	if err != nil {
		log.Fatalf("Error loading CSV string: %v", err)
	}

	defer df.Close()

	fmt.Println("DataFrame created successfully from csv string!")
}
