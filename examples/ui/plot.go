// Package main is the entry point for the example application that demonstrates
// the usage of the custom Plot widget from the viz package. This example shows
// how to create a live-updating plot using dynamic data.
package main

import (
	"math/rand"
	"time"
	"github.com/aggnr/bluejay/viz"
)

// main is the entry point of the application. It initializes the x and y data,
// creates a channel for dynamic data updates, and starts the plot display.
func main() {
	// Initial x-axis data points
	xData := []float64{0, 1, 2, 3, 4, 5}

	// Initial y-axis data points
	yData := []float64{0.1, 0.5, 0.9, 0.4, 0.7, 1.0}

	// Channel to send new data points for dynamic updates
	dataChan := make(chan float64)

	// Goroutine to generate new data points every second and send them to the channel
	go func() {
		for range time.Tick(time.Second) {
			dataChan <- rand.Float64()
		}
	}()

	// Call ShowPlot to display the plot with initial data and dynamic updates
	viz.ShowPlot(xData, yData, "Time (s)", "Value", dataChan)
}