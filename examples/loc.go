package main

import (
	"fmt"
	"log"

	"github.com/aggnr/bluejay/dataframe"
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

	df, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	rows, err := df.Loc(1)
	if err != nil {
		log.Fatalf("Error retrieving rows for index 1: %v", err)
	}

	fmt.Println("Retrieved rows for index 1:")
	for _, row := range rows {
		fmt.Println(row)
	}

	rows, err = df.Loc(0, 1)
	if err != nil {
		log.Fatalf("Error retrieving rows for index 0 and 1: %v", err)
	}

	fmt.Println("Retrieved rows for index 0 and 1:")
	for _, row := range rows {
		fmt.Println(row)
	}
}