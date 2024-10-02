package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
	"github.com/aggnr/bluejay/dataframe"
)

type Person struct {
	Name string
	Age  int
	City string
}

func main() {
	scale := 10000000
	// Create a slice of structs with 1 million entries
	data := make([]Person, scale)
	for i := 0; i < scale; i++ {
		data[i] = Person{
			Name: fmt.Sprintf("Person%d", i),
			Age:  i % 100,
			City: fmt.Sprintf("City%d", i % 1000),
		}
	}

	// Measure the time to create a new DataFrame with storage on disk
	start := time.Now()
	df, err := dataframe.NewDataFrame(data, "./data", false) // true for disk storage
	if err != nil {
		fmt.Println("Error creating DataFrame:", err)
		return
	}
	fmt.Printf("Time to create DataFrame: %.6f ms\n", float64(time.Since(start).Microseconds())/1000)

	// Sequence generator
	idSeq := len(data)

	// Measure the average time to insert a new row in parallel
	insertTimes := make([]time.Duration, 100)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			newPerson := Person{"NewPerson", 30, "NewCity"}
			start := time.Now()
			df.InsertRow(idSeq, newPerson)
			duration := time.Since(start)
			mu.Lock()
			insertTimes[i] = duration
			mu.Unlock()
			idSeq++
		}(i)
	}
	wg.Wait()

	var totalInsertTime time.Duration
	for _, t := range insertTimes {
		totalInsertTime += t
	}
	fmt.Printf("Average time to insert a new row: %.6f ms\n", float64((totalInsertTime / time.Duration(len(insertTimes))).Microseconds())/1000)

	// Measure the average time to search for a random row
	searchTimes := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		randomID := rand.Intn(scale)
		start := time.Now()
		_, err = df.ReadRow(randomID)
		if err != nil {
			fmt.Println("Error reading row:", err)
			return
		}
		searchTimes[i] = time.Since(start)
	}
	var totalSearchTime time.Duration
	for _, t := range searchTimes {
		totalSearchTime += t
	}
	fmt.Printf("Average time to read a random row: %.6f ms\n", float64((totalSearchTime / time.Duration(len(searchTimes))).Microseconds())/1000)
}