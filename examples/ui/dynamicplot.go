package main

import (
	"log"
	"math/rand"
	"sort"
	"time"
	"github.com/aggnr/bluejay/dataframe"
	"github.com/aggnr/bluejay/viz"
)

func main() {
	type DataPoint struct {
		Time  float64
		Value float64
	}

	// Initial data points
	dataPoints := []DataPoint{
		{0, 0.1},
		{1, 0.5},
		{2, 0.9},
		{3, 0.4},
		{4, 0.7},
		{5, 1.0},
	}

	df, err := dataframe.NewDataFrame(dataPoints)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}

	title := "Dynamic Plot"

	// Channel to send new data points for dynamic updates
	dataChan := make(chan [2]float64)

	// Goroutine to generate new data points every second and send them to the channel
	go func() {
		newTime := dataPoints[len(dataPoints)-1].Time + 1
		for range time.Tick(time.Second) {
			newValue := rand.Float64()
			dataChan <- [2]float64{newTime, newValue}
			dataPoints = append(dataPoints, DataPoint{newTime, newValue})
			newTime += 1
		}
	}()

	// Extract data for plotting
	xData := []float64{}
	yData := []float64{}
	for _, row := range df.Data {
		rowMap := row.(map[string]interface{})
		xVal, ok1 := dataframe.ToFloat64(rowMap["Time"])
		yVal, ok2 := dataframe.ToFloat64(rowMap["Value"])
		if (!ok1 || !ok2) {
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
	viz.ShowPlot(xData, yData, "Time (s)", "Value", title, dataChan)
}