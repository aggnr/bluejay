package main

import (
	"log"
	"github.com/aggnr/bluejay/dataframe"
)

func main() {

	csvString := `Name,Age,Salary,IsMarried
John,30,50000.50,true
,,60000.75,false`

	type Person struct {
		Name      string
		Age       int
		Salary    float64
		IsMarried bool
	}

	df, err := dataframe.ReadCSVFromString(csvString, &Person{})
	if err != nil {
		log.Fatalf("Error reading CSV: %v", err)
	}

	df.Display()
}