//go:build ignoreme
// +build ignoreme
package main

import (
	"log"
	"fmt"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Define two structs for the data
	type Person struct {
		ID   int
		Name string
		Age  int
	}

	type Job struct {
		ID       int
		PersonID int
		Title    string
		Salary   float64
	}

	// Create sample data
	people := []Person{
		{ID: 1, Name: "John", Age: 30},
		{ID: 2, Name: "Jane", Age: 25},
	}

	jobs := []Job{
		{ID: 1, PersonID: 1, Title: "Engineer", Salary: 70000},
		{ID: 2, PersonID: 2, Title: "Doctor", Salary: 80000},
	}

	// Create DataFrames for the sample data
	dfPeople, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame for people: %v", err)
	}

	dfJobs, err := dataframe.NewDataFrame(jobs)
	if err != nil {
		log.Fatalf("Error creating DataFrame for jobs: %v", err)
	}

	// Perform an inner join on the two DataFrames
	joinedDF, err := dfPeople.Join(dfJobs, "inner", []string{"ID"})
	if err != nil {
		log.Fatalf("Error joining DataFrames: %v", err)
	}

	// Display the joined DataFrame
	if err := joinedDF.Display(); err != nil {
		log.Fatalf("Error displaying joined DataFrame: %v", err)
	}

	defer dataframe.CleanUP()
}