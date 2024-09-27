package viz

import (
	"fmt"
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

	// Create a grid for the matrix
	grid := container.NewGridWithColumns(len(columns) + 1)

	// Add an empty cell at the top-left corner
	grid.Add(canvas.NewText("", color.White))

	// Add column names to the top
	for _, colName := range columns {
		label := canvas.NewText(colName, color.White)
		label.Alignment = fyne.TextAlignCenter
		grid.Add(label)
	}

	// Add rows with row names and matrix values
	for i := range matrix {
		// Add row name
		rowLabel := canvas.NewText(columns[i], color.White)
		rowLabel.Alignment = fyne.TextAlignCenter
		grid.Add(rowLabel)

		for j := range matrix[i] {
			value := matrix[i][j]
			col := getColorForValue(value)
			rect := canvas.NewRectangle(col)
			rect.SetMinSize(fyne.NewSize(50, 50))

			// Create a text label for the value
			label := canvas.NewText(fmt.Sprintf("%.2f", value), color.White)
			label.Alignment = fyne.TextAlignCenter

			// Create a container with the rectangle and the label
			cell := container.NewMax(rect, label)
			grid.Add(cell)
		}
	}

	// Create a legend
	legend := createLegend()

	// Create a vertical container with the grid and the legend
	content := container.NewVBox(grid, legend)

	w.SetContent(content)
	w.Resize(fyne.NewSize(600, 700))
	w.ShowAndRun()
}

// getColorForValue returns a color based on the correlation value using a Seaborn-like palette
func getColorForValue(value float64) color.Color {
	// Normalize the value to be between 0 and 1
	normalized := (value + 1) / 2

	// Seaborn-like color palette (blue to white to red)
	red := uint8(255 * normalized)
	green := uint8(255 * (1 - math.Abs(normalized-0.5)*2))
	blue := uint8(255 * (1 - normalized))

	return color.NRGBA{R: red, G: green, B: blue, A: 255}
}

// createLegend creates a legend for the correlation matrix
func createLegend() *fyne.Container {
	legend := container.NewHBox()

	// Add color rectangles and labels to the legend
	for i := -10; i <= 10; i++ {
		value := float64(i) / 10
		col := getColorForValue(value)
		rect := canvas.NewRectangle(col)
		rect.SetMinSize(fyne.NewSize(20, 20))

		label := canvas.NewText(fmt.Sprintf("%.1f", value), color.White)
		label.Alignment = fyne.TextAlignCenter

		legend.Add(container.NewVBox(rect, label))
	}

	return legend
}