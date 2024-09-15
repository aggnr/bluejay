// Description: This example demonstrates how to retrieve rows from a DataFrame using the Loc method.
//go:build ignoreme
// +build ignoreme

package main

import (
	"fmt"
	"github.com/aggnr/bluejay"
)

func main() {

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

	df, _ := bluejay.NewDataFrame(people)

	defer df.Close()

	rows, _ := df.Loc(2)

	fmt.Println("Retrieved rows for index 2:")
	for _, row := range rows {
		fmt.Println(row)
	}

	rows, _ = df.Loc(1, 2)

	fmt.Println("Retrieved rows for index 1 and 2:")
	for _, row := range rows {
		fmt.Println(row)
	}
}
