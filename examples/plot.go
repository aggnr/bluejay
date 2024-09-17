//go:build ignoreme
// +build ignoreme

package main

import "github.com/aggnr/bluejay"

func main() {
	// Example correlation matrix
	matrix := [][]float64{
		{1, 0.8, 0.6},
		{0.8, 1, 0.4},
		{0.6, 0.4, 1},
	}

	// Plot the correlation matrix and save it as a PNG file
	err := PlotCorrelationMatrix(matrix, "correlation_matrix.png")
	if err != nil {
		panic(err)
	}
}

