package main

import (
"image"
"image/color"
"image/png"
"math"
"os"
)

// PlotCorrelationMatrix plots the correlation matrix and saves it as a PNG file.
func PlotCorrelationMatrix(matrix [][]float64, filename string) error {
	// Define the size of each cell in the matrix
	cellSize := 50
	width := len(matrix) * cellSize
	height := len(matrix) * cellSize

	// Create a new image with the calculated width and height
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// Iterate over the matrix and fill the image with colors based on the correlation values
	for i := range matrix {
		for j := range matrix[i] {
			// Map the correlation value to a color
			corr := matrix[i][j]
			col := correlationToColor(corr)

			// Fill the corresponding cell in the image
			for x := i * cellSize; x < (i+1)*cellSize; x++ {
				for y := j * cellSize; y < (j+1)*cellSize; y++ {
					img.Set(x, y, col)
				}
			}
		}
	}

	// Create the output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Encode the image to PNG format and save it to the file
	return png.Encode(file, img)
}

// correlationToColor maps a correlation value to a color.
func correlationToColor(corr float64) color.Color {
	// Normalize the correlation value to the range [0, 1]
	norm := (corr + 1) / 2

	// Map the normalized value to a color gradient (blue to red)
	r := uint8(255 * norm)
	b := uint8(255 * (1 - norm))
	return color.RGBA{R: r, G: 0, B: b, A: 255}
}