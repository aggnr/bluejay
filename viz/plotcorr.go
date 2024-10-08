package viz

import (
	"fmt"
	"image/color"
	"math"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
)

// PlotCorrMat displays the correlation matrix using the fyne package
func PlotCorrMat(matrix [][]float64, columns []string) {
	a := app.New()
	a.Settings().SetTheme(theme.LightTheme())
	w := a.NewWindow("Correlation Matrix")

	// Calculate the dynamic width and height based on the number of columns and rows
	columnWidth := 50
	rowHeight := 50
	padding := 20
	dynamicWidth := (len(columns) + 1) * columnWidth + 2 * padding
	dynamicHeight := (len(matrix) + 1) * rowHeight + 2 * padding

	// Create a grid for the matrix
	grid := container.NewGridWithColumns(len(columns) + 1)

	// Add an empty cell at the top-left corner
	grid.Add(canvas.NewText("", color.Black))

	// Add column names to the top
	for _, colName := range columns {
		label := canvas.NewText(colName, color.Black)
		label.Alignment = fyne.TextAlignCenter
		grid.Add(label)
	}

	// Add rows with row names and matrix values
	for i := range matrix {
		// Add row name
		rowLabel := canvas.NewText(columns[i], color.Black)
		rowLabel.Alignment = fyne.TextAlignCenter
		grid.Add(rowLabel)

		for j := range matrix[i] {
			value := matrix[i][j]
			col := getColorForValue(value)
			rect := canvas.NewRectangle(col)
			rect.SetMinSize(fyne.NewSize(float32(columnWidth), float32(rowHeight)))

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
	w.Resize(fyne.NewSize(float32(dynamicWidth), float32(dynamicHeight)))
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

		label := canvas.NewText(fmt.Sprintf("%.1f", value), color.Black)
		label.Alignment = fyne.TextAlignCenter

		legend.Add(container.NewVBox(rect, label))
	}

	return legend
}