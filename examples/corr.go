// Description: This example demonstrates how to use the Corr method to calculate the correlation matrix of a DataFrame.

//go:build ignoreme
// +build ignoreme
package main

import (
	"log"
	"github.com/aggnr/bluejay"
)

// Define the struct that matches the CSV data structure
type SampleData struct {
	Age        int     `json:"Age"`
	Salary     float64 `json:"Salary"`
	Experience int     `json:"Experience"`
}

func main() {
	// CSV data as a string
	csvString := `Age,Salary,Experience
25,50000,2
30,60000,5
35,70000,8
40,80000,11
45,90000,14
50,100000,17
55,110000,20
60,120000,23
65,130000,26
70,140000,29`

	// Read the CSV data into a DataFrame
	df, err := bluejay.ReadCSVFromString(csvString, &SampleData{})
	if err != nil {
		log.Fatalf("Failed to read CSV data: %v", err)
	}
	defer df.Close()

	// Calculate the correlation matrix
	corrDF, err := df.Corr()
	if err != nil {
		log.Fatalf("Failed to calculate correlation matrix: %v", err)
	}

	// Print the correlation matrix using the Display method
	corrDF.DisplayCorr()
}

