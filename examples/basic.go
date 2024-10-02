package main

import (
	"fmt"
	"github.com/aggnr/bluejay/dataframe"
)

type Person struct {
	Name string
	Age  int
	City string
}

func main() {
	// Create a slice of structs
	data := []Person{
		{"Alice", 30, "New York"},
		{"Bob", 25, "San Francisco"},
		{"Charlie", 35, "Los Angeles"},
	}

	// Create a new DataFrame from the slice of structs
	df, err := dataframe.NewDataFrame(data)
	if err != nil {
		fmt.Println("Error creating DataFrame:", err)
		return
	}

	// Sequence generator
	idSeq := len(data)

	// Insert a new row with a sequence id
	newPerson := Person{"David", 40, "Chicago"}
	df.InsertRow(idSeq, newPerson)
	idSeq++

	// Read a row
	row, err := df.ReadRow(1)
	if err != nil {
		fmt.Println("Error reading row:", err)
		return
	}
	fmt.Println("Row 1:", row)

	// Update a row
	newValues := map[string]interface{}{
		"Age":  26,
		"City": "Seattle",
	}
	err = df.UpdateRow(1, newValues)
	if err != nil {
		fmt.Println("Error updating row:", err)
		return
	}

	// Read the updated row
	row, err = df.ReadRow(1)
	if err != nil {
		fmt.Println("Error reading row:", err)
		return
	}
	fmt.Println("Updated Row 1:", row)

	// Delete a row
	err = df.DeleteRow(1)
	if err != nil {
		fmt.Println("Error deleting row:", err)
		return
	}

	// Try to read the deleted row
	row, err = df.ReadRow(1)
	if err != nil {
		fmt.Println("Error reading row:", err)
	} else {
		fmt.Println("Row 2:", row)
	}
}