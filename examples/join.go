package main

import (
	"log"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {
	// Define two structs for the data
	type Person struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	type Job struct {
		ID       int     `json:"id"`
		PersonID int     `json:"person_id"`
		Title    string  `json:"title"`
		Salary   float64 `json:"salary"`
	}

	// Create sample data with mismatched IDs
	people := []Person{
		{ID: 1, Name: "John", Age: 30},
		{ID: 2, Name: "Jane", Age: 25},
		{ID: 3, Name: "Doe", Age: 40},
	}

	jobs := []Job{
		{ID: 1, PersonID: 4, Title: "Engineer", Salary: 70000},
		{ID: 2, PersonID: 5, Title: "Doctor", Salary: 80000},
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
	innerJoinDF, err := dfPeople.Join(dfJobs, "inner", []string{"ID", "PersonID"})
	if err != nil {
		log.Fatalf("Error performing inner join: %v", err)
	}
	log.Println("Inner Join Result with mismatched IDs:")
	if err := innerJoinDF.Display(); err != nil {
		log.Fatalf("Error displaying inner join DataFrame: %v", err)
	}

	// Perform a left join on the two DataFrames
	leftJoinDF, err := dfPeople.Join(dfJobs, "left", []string{"ID", "PersonID"})
	if err != nil {
		log.Fatalf("Error performing left join: %v", err)
	}
	log.Println("Left Join Result with mismatched IDs:")
	if err := leftJoinDF.Display(); err != nil {
		log.Fatalf("Error displaying left join DataFrame: %v", err)
	}

	// Perform a right join on the two DataFrames
	rightJoinDF, err := dfPeople.Join(dfJobs, "right", []string{"ID", "PersonID"})
	if err != nil {
		log.Fatalf("Error performing right join: %v", err)
	}
	log.Println("Right Join Result with mismatched IDs:")
	if err := rightJoinDF.Display(); err != nil {
		log.Fatalf("Error displaying right join DataFrame: %v", err)
	}

	// Perform an outer join on the two DataFrames
	outerJoinDF, err := dfPeople.Join(dfJobs, "outer", []string{"ID", "PersonID"})
	if err != nil {
		log.Fatalf("Error performing outer join: %v", err)
	}
	log.Println("Outer Join Result with mismatched IDs:")
	if err := outerJoinDF.Display(); err != nil {
		log.Fatalf("Error displaying outer join DataFrame: %v", err)
	}
}