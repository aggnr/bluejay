package main

import (
	"log"
	"sort"
	"github.com/aggnr/bluejay/dataframe"
	"github.com/aggnr/bluejay/viz"
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
		{"Alice", 28, 7.20, true},
		{"Bob", 35, 8.50, false},
		{"Charlie", 40, 9.00, true},
		{"Diana", 22, 4.75, false},
		{"Eve", 29, 6.00, true},
		{"Frank", 33, 7.80, false},
		{"Grace", 27, 5.90, true},
		{"Hank", 31, 6.40, false},
	}

	df, err := dataframe.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	// Extract data for plotting
	xData := []float64{}
	yData := []float64{}
	for _, row := range df.Data {
		rowMap := row.(map[string]interface{})
		xVal, ok1 := dataframe.ToFloat64(rowMap["Age"])
		yVal, ok2 := dataframe.ToFloat64(rowMap["Salary"])
		if !ok1 || !ok2 {
			log.Fatalf("Non-numeric value encountered")
		}
		xData = append(xData, xVal)
		yData = append(yData, yVal)
	}

	// Sort xData and rearrange yData accordingly
	type dataPair struct {
		x float64
		y float64
	}
	pairs := make([]dataPair, len(xData))
	for i := range xData {
		pairs[i] = dataPair{xData[i], yData[i]}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].x < pairs[j].x
	})
	for i := range pairs {
		xData[i] = pairs[i].x
		yData[i] = pairs[i].y
	}

	// Call ShowPlot to display the plot with initial data and dynamic updates
	viz.ShowPlot(xData, yData, "Age", "Salary", "Age vs Salary", nil)
}