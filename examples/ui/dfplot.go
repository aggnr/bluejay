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

	df, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Call ShowPlot to display the plot with initial data and dynamic updates
	if err := df.ShowPlot("Age", "Salary", ""); err != nil {
		log.Fatalf("Error showing plot: %v", err)
	}
}