package main

import (
	"math/rand"
	"time"
	"github.com/aggnr/bluejay/viz"
)

func main() {
	// Initial x-axis data points
	xData := []float64{0, 1, 2, 3, 4, 5}

	// Initial y-axis data points
	yData := []float64{0.1, 0.5, 0.9, 0.4, 0.7, 1.0}

	title := "Dynamic Plot"

	// Channel to send new data points for dynamic updates
	dataChan := make(chan [2]float64)

	// Goroutine to generate new data points every second and send them to the channel
	go func() {
		for range time.Tick(time.Second) {
			newX := float64(len(xData))
			newY := rand.Float64()
			dataChan <- [2]float64{newX, newY}
			xData = append(xData, newX)
			yData = append(yData, newY)
		}
	}()

	// Call ShowPlot to display the plot with initial data and dynamic updates
	viz.ShowPlot(xData, yData, "Time (s)", "Value", title, dataChan)
}