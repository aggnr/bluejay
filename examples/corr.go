package main

import (
	"fmt"
	"log"
	"github.com/aggnr/bluejay/dataframe"
	"github.com/aggnr/bluejay/viz"
)

// Define the struct that matches the CSV data structure
type SampleData struct {
	Age        int     `json:"Age"`
	Salary     float64 `json:"Salary"`
	Experience int     `json:"Experience"`
	Height     float64 `json:"Height"`
	Weight     float64 `json:"Weight"`
	Score      float64 `json:"Score"`
}

func main() {

	// CSV data as a string with mixed correlations
	csvString := `Age,Salary,Experience,Height,Weight,Score
25,50000,2,170,70,85
30,60000,5,175,75,88
35,70000,8,180,80,90
40,80000,12,185,85,92
45,90000,10,190,90,94
50,100000,20,195,95,96
55,110000,25,200,100,98
60,120000,30,205,105,100
65,130000,26,210,110,102
70,140000,29,215,115,104
75,135000,28,220,120,106
80,130000,27,225,125,108
85,125000,26,230,130,110
90,120000,25,235,135,112
95,115000,24,240,140,114`

	// Read the CSV data into a DataFrame
	df, err := dataframe.ReadCSVFromString(csvString, &SampleData{})
	if err != nil {
		log.Fatalf("Failed to read CSV data: %v", err)
	}

	// Calculate the correlation matrix
	corrDF, err := df.Corr()
	if err != nil {
		log.Fatalf("Failed to calculate correlation matrix: %v", err)
	}

	// Convert the correlation DataFrame to a 2D slice of float64 and get column names
	corrMatrix, columns, err := corrDF.ToMatrix()
	if err != nil {
		log.Fatalf("Failed to convert correlation DataFrame to matrix: %v", err)
	}

	// Plot the correlation matrix
	viz.PlotCorrMat(corrMatrix, columns)

	// Display the correlation DataFrame
	fmt.Println("Correlation DataFrame:")
	corrDF.Display()
}