package main

import (
	"fmt"
	"log"
	"math/rand"
	//"net/http"
	//_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
	"github.com/aggnr/bluejay/dataframe"
)

type Person struct {
	Name    string
	Age     int
	City    string
	Field1  string
	Field2  string
	Field3  string
	Field4  string
	Field5  string
	Field6  string
	Field7  string
	Field8  string
	Field9  string
	Field10 string
	Field11 string
	Field12 string
	Field13 string
	Field14 string
	Field15 string
	Field16 string
	Field17 string
	Field18 string
	Field19 string
	Field20 string
	Field21 string
	Field22 string
	Field23 string
	Field24 string
	Field25 string
	Field26 string
	Field27 string
	Field28 string
	Field29 string
	Field30 string
	Field31 string
	Field32 string
	Field33 string
	Field34 string
	Field35 string
	Field36 string
	Field37 string
	Field38 string
	Field39 string
	Field40 string
	Field41 string
	Field42 string
	Field43 string
	Field44 string
	Field45 string
	Field46 string
	Field47 string
	Field48 string
	Field49 string
	Field50 string
	Field51 string
	Field52 string
	Field53 string
	Field54 string
	Field55 string
	Field56 string
	Field57 string
	Field58 string
	Field59 string
	Field60 string
	Field61 string
	Field62 string
	Field63 string
	Field64 string
	Field65 string
	Field66 string
	Field67 string
	Field68 string
	Field69 string
	Field70 string
	Field71 string
	Field72 string
	Field73 string
	Field74 string
	Field75 string
	Field76 string
	Field77 string
	Field78 string
	Field79 string
	Field80 string
	Field81 string
	Field82 string
	Field83 string
	Field84 string
	Field85 string
	Field86 string
	Field87 string
	Field88 string
	Field89 string
	Field90 string
	Field91 string
	Field92 string
	Field93 string
	Field94 string
	Field95 string
	Field96 string
	Field97 string
	Field98 string
	Field99 string
	Field100 string
}

func main() {
	//// Start HTTP server for pprof
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	// Profile memory usage before the operations
	//profileMemory("memprofile_before.prof")

	scale := 1000000
	// Create a slice of structs with 2 million entries
	data := make([]Person, scale)
	for i := 0; i < scale; i++ {
		data[i] = Person{
			Name:    fmt.Sprintf("Person%d", i),
			Age:     i % 100,
			City:    fmt.Sprintf("City%d", i%1000),
			Field1:  "Field1",
			Field2:  "Field2",
			Field3:  "Field3",
			Field4:  "Field4",
			Field5:  "Field5",
			Field6:  "Field6",
			Field7:  "Field7",
			Field8:  "Field8",
			Field9:  "Field9",
			Field10: "Field10",
			Field11: "Field11",
			Field12: "Field12",
			Field13: "Field13",
			Field14: "Field14",
			Field15: "Field15",
			Field16: "Field16",
			Field17: "Field17",
			Field18: "Field18",
			Field19: "Field19",
			Field20: "Field20",
			Field21: "Field21",
			Field22: "Field22",
			Field23: "Field23",
			Field24: "Field24",
			Field25: "Field25",
			Field26: "Field26",
			Field27: "Field27",
			Field28: "Field28",
			Field29: "Field29",
			Field30: "Field30",
			Field31: "Field31",
			Field32: "Field32",
			Field33: "Field33",
			Field34: "Field34",
			Field35: "Field35",
			Field36: "Field36",
			Field37: "Field37",
			Field38: "Field38",
			Field39: "Field39",
			Field40: "Field40",
			Field41: "Field41",
			Field42: "Field42",
			Field43: "Field43",
			Field44: "Field44",
			Field45: "Field45",
			Field46: "Field46",
			Field47: "Field47",
			Field48: "Field48",
			Field49: "Field49",
			Field50: "Field50",
			Field51: "Field51",
			Field52: "Field52",
			Field53: "Field53",
			Field54: "Field54",
			Field55: "Field55",
			Field56: "Field56",
			Field57: "Field57",
			Field58: "Field58",
			Field59: "Field59",
			Field60: "Field60",
			Field61: "Field61",
			Field62: "Field62",
			Field63: "Field63",
			Field64: "Field64",
			Field65: "Field65",
			Field66: "Field66",
			Field67: "Field67",
			Field68: "Field68",
			Field69: "Field69",
			Field70: "Field70",
			Field71: "Field71",
			Field72: "Field72",
			Field73: "Field73",
			Field74: "Field74",
			Field75: "Field75",
			Field76: "Field76",
			Field77: "Field77",
			Field78: "Field78",
			Field79: "Field79",
			Field80: "Field80",
			Field81: "Field81",
			Field82: "Field82",
			Field83: "Field83",
			Field84: "Field84",
			Field85: "Field85",
			Field86: "Field86",
			Field87: "Field87",
			Field88: "Field88",
			Field89: "Field89",
			Field90: "Field90",
			Field91: "Field91",
			Field92: "Field92",
			Field93: "Field93",
			Field94: "Field94",
			Field95: "Field95",
			Field96: "Field96",
			Field97: "Field97",
			Field98: "Field98",
			Field99: "Field99",
			Field100: "Field100",
		}
	}

	// Measure the time to create a new DataFrame with storage on disk
	start := time.Now()
	df, err := dataframe.NewDataFrame(data) // Adjusted to match the expected signature
	if err != nil {
		fmt.Println("Error creating DataFrame:", err)
		return
	}
	fmt.Printf("Time to create DataFrame: %.6f ms\n", float64(time.Since(start).Microseconds())/1000)

	// Measure the average time to insert a new row in parallel
	var wg sync.WaitGroup
	insertTimes := make([]time.Duration, 100)
	idSeq := len(data)
	idSeqChan := make(chan int, 100)

	for i := 0; i < 100; i++ {
		idSeqChan <- idSeq + i
	}
	close(idSeqChan)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			newPerson := Person{
				Name:    "NewPerson",
				Age:     30,
				City:    "NewCity",
				Field1:  "Field1",
				Field2:  "Field2",
				Field3:  "Field3",
				Field4:  "Field4",
				Field5:  "Field5",
				Field6:  "Field6",
				Field7:  "Field7",
				Field8:  "Field8",
				Field9:  "Field9",
				Field10: "Field10",
				Field11: "Field11",
				Field12: "Field12",
				Field13: "Field13",
				Field14: "Field14",
				Field15: "Field15",
				Field16: "Field16",
				Field17: "Field17",
				Field18: "Field18",
				Field19: "Field19",
				Field20: "Field20",
				Field21: "Field21",
				Field22: "Field22",
				Field23: "Field23",
				Field24: "Field24",
				Field25: "Field25",
				Field26: "Field26",
				Field27: "Field27",
				Field28: "Field28",
				Field29: "Field29",
				Field30: "Field30",
				Field31: "Field31",
				Field32: "Field32",
				Field33: "Field33",
				Field34: "Field34",
				Field35: "Field35",
				Field36: "Field36",
				Field37: "Field37",
				Field38: "Field38",
				Field39: "Field39",
				Field40: "Field40",
				Field41: "Field41",
				Field42: "Field42",
				Field43: "Field43",
				Field44: "Field44",
				Field45: "Field45",
				Field46: "Field46",
				Field47: "Field47",
				Field48: "Field48",
				Field49: "Field49",
				Field50: "Field50",
				Field51: "Field51",
				Field52: "Field52",
				Field53: "Field53",
				Field54: "Field54",
				Field55: "Field55",
				Field56: "Field56",
				Field57: "Field57",
				Field58: "Field58",
				Field59: "Field59",
				Field60: "Field60",
				Field61: "Field61",
				Field62: "Field62",
				Field63: "Field63",
				Field64: "Field64",
				Field65: "Field65",
				Field66: "Field66",
				Field67: "Field67",
				Field68: "Field68",
				Field69: "Field69",
				Field70: "Field70",
				Field71: "Field71",
				Field72: "Field72",
				Field73: "Field73",
				Field74: "Field74",
				Field75: "Field75",
				Field76: "Field76",
				Field77: "Field77",
				Field78: "Field78",
				Field79: "Field79",
				Field80: "Field80",
				Field81: "Field81",
				Field82: "Field82",
				Field83: "Field83",
				Field84: "Field84",
				Field85: "Field85",
				Field86: "Field86",
				Field87: "Field87",
				Field88: "Field88",
				Field89: "Field89",
				Field90: "Field90",
				Field91: "Field91",
				Field92: "Field92",
				Field93: "Field93",
				Field94: "Field94",
				Field95: "Field95",
				Field96: "Field96",
				Field97: "Field97",
				Field98: "Field98",
				Field99: "Field99",
				Field100: "Field100",
			}
			start := time.Now()
			id := <-idSeqChan
			df.InsertRow(id, newPerson)
			duration := time.Since(start)
			insertTimes[i] = duration
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

	df.Close()

	// Profile memory usage after the operations
	//profileMemory("memprofile_after.prof")
}

func profileMemory(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer f.Close()

	// Run garbage collection to get up-to-date statistics
	runtime.GC()

	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}