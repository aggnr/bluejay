package main

import (
	"log"
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
		{"John", 30, 5.50, true},
		{"Jane", 25, 6.75, false},
	}

	df, err := bluejay.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}
	defer df.Close()

	// Call ShowPlot to display the plot with initial data and dynamic updates
	if err := df.ShowPlot("Age", "Salary", ""); err != nil {
		log.Fatalf("Error showing plot: %v", err)
	}
}