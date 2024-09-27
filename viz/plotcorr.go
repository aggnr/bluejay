package viz

import (
	"image/color"
	"math"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
)

// PlotCorrMat displays the correlation matrix using the fyne package
func PlotCorrMat(matrix [][]float64, columns []string) {
	a := app.New()
	w := a.NewWindow("Correlation Matrix")

	grid := container.NewGridWithColumns(len(columns))

	for i := range matrix {
		for j := range matrix[i] {
			value := matrix[i][j]
			color := getColorForValue(value)
			rect := canvas.NewRectangle(color)
			rect.SetMinSize(fyne.NewSize(50, 50))
			grid.Add(rect)
		}
	}

	w.SetContent(grid)
	w.Resize(fyne.NewSize(600, 600))
	w.ShowAndRun()
}

// getColorForValue returns a color based on the correlation value
func getColorForValue(value float64) color.Color {
	// Normalize the value to be between 0 and 1
	normalized := (value + 1) / 2
	// Convert the normalized value to a color
	red := uint8(255 * (1 - normalized))
	blue := uint8(255 * normalized)
	return color.RGBA{R: red, G: 0, B: blue, A: 255}
}