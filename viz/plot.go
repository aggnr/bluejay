package viz

import (
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"image/color"
	"strconv"
	"fmt"
)

// Plot is a custom widget to display live data as a plot
type Plot struct {
	widget.BaseWidget
	xData      []float64
	yData      []float64
	xAxisLabel string
	yAxisLabel string
}

// NewPlot creates a new Plot widget with axis labels
func NewPlot(xAxisLabel, yAxisLabel string) *Plot {
	plot := &Plot{xAxisLabel: xAxisLabel, yAxisLabel: yAxisLabel}
	plot.ExtendBaseWidget(plot)
	return plot
}

// UpdateData updates the data of the plot
func (p *Plot) UpdateData(newXData, newYData float64) {
	p.xData = append(p.xData, newXData)
	p.yData = append(p.yData, newYData)
	p.Refresh()
}

// CreateRenderer implements fyne.WidgetRenderer
func (p *Plot) CreateRenderer() fyne.WidgetRenderer {
	xAxis := canvas.NewLine(color.Black)
	yAxis := canvas.NewLine(color.Black)
	xAxisLabel := canvas.NewText(p.xAxisLabel, color.Black)
	yAxisLabel := canvas.NewText(p.yAxisLabel, color.Black)
	yAxisLabel.TextStyle = fyne.TextStyle{Bold: true}
	return &plotRenderer{plot: p, xAxis: xAxis, yAxis: yAxis, xAxisLabel: xAxisLabel, yAxisLabel: yAxisLabel}
}

// Add a new field for the gridlines in the plotRenderer struct
type plotRenderer struct {
	plot       *Plot
	xAxis      *canvas.Line
	yAxis      *canvas.Line
	xAxisLabel *canvas.Text
	yAxisLabel *canvas.Text
	lines      []fyne.CanvasObject
	xScale     []fyne.CanvasObject
	yScale     []fyne.CanvasObject
	points     []fyne.CanvasObject
	gridlines  []fyne.CanvasObject // Add this field
}

// Layout the gridlines in the Layout method
func (r *plotRenderer) Layout(size fyne.Size) {
	padding := float32(40) // Add padding of 40 units
	r.xAxis.Position1 = fyne.NewPos(padding, size.Height-padding)
	r.xAxis.Position2 = fyne.NewPos(size.Width-padding, size.Height-padding)
	r.yAxis.Position1 = fyne.NewPos(padding, padding)
	r.yAxis.Position2 = fyne.NewPos(padding, size.Height-padding)
	r.xAxisLabel.Move(fyne.NewPos(size.Width/2, size.Height-padding+5))
	r.yAxisLabel.Move(fyne.NewPos(padding-30, size.Height/2))
	r.yAxisLabel.TextSize = 12
	r.yAxisLabel.Alignment = fyne.TextAlignCenter
	r.yAxisLabel.TextStyle = fyne.TextStyle{Italic: true}

	// Find the min and max values for x and y
	minX, maxX := r.plot.xData[0], r.plot.xData[0]
	minY, maxY := r.plot.yData[0], r.plot.yData[0]
	for _, x := range r.plot.xData {
		if x < minX {
			minX = x
		}
		if x > maxX {
			maxX = x
		}
	}
	for _, y := range r.plot.yData {
		if y < minY {
			minY = y
		}
		if y > maxY {
			maxY = y
		}
	}

	// Layout lines
	for i, line := range r.lines {
		if i < len(r.plot.xData)-1 {
			x1 := padding + (float32(r.plot.xData[i]-minX)/float32(maxX-minX))*(size.Width-2*padding)
			y1 := size.Height - padding - (float32(r.plot.yData[i]-minY)/float32(maxY-minY))*(size.Height-2*padding)
			x2 := padding + (float32(r.plot.xData[i+1]-minX)/float32(maxX-minX))*(size.Width-2*padding)
			y2 := size.Height - padding - (float32(r.plot.yData[i+1]-minY)/float32(maxY-minY))*(size.Height-2*padding)
			line.(*canvas.Line).Position1 = fyne.NewPos(x1, y1)
			line.(*canvas.Line).Position2 = fyne.NewPos(x2, y2)
		}
	}

	// Layout x-axis scale with actual values
	for i, label := range r.xScale {
		x := padding + float32(i)*(size.Width-2*padding)/float32(len(r.plot.xData)-1)
		if textLabel, ok := label.(*canvas.Text); ok {
			textLabel.Text = fmt.Sprintf("%.2f", r.plot.xData[i])
			textLabel.Move(fyne.NewPos(x, size.Height-padding+5))
		}
	}

	// Layout y-axis scale with actual values
	for i, label := range r.yScale {
		y := size.Height - padding - float32(i)*(size.Height-2*padding)/10
		if textLabel, ok := label.(*canvas.Text); ok {
			textLabel.Text = fmt.Sprintf("%.2f", minY+float64(i)*(maxY-minY)/10)
			textLabel.Move(fyne.NewPos(padding-30, y))
		}
	}

	// Layout points
	for i, point := range r.points {
		x := padding + (float32(r.plot.xData[i]-minX)/float32(maxX-minX))*(size.Width-2*padding)
		y := size.Height - padding - (float32(r.plot.yData[i]-minY)/float32(maxY-minY))*(size.Height-2*padding)
		point.(*canvas.Circle).Move(fyne.NewPos(x, y))
	}

	// Layout horizontal gridlines
	for i, gridline := range r.gridlines {
		y := size.Height - padding - float32(i)*(size.Height-2*padding)/10
		gridline.(*canvas.Line).Position1 = fyne.NewPos(padding, y)
		gridline.(*canvas.Line).Position2 = fyne.NewPos(size.Width-padding, y)
	}
}

// MinSize implements fyne.WidgetRenderer
func (r *plotRenderer) MinSize() fyne.Size {
	return fyne.NewSize(800, 640)
}

// Create the gridlines in the Refresh method
func (r *plotRenderer) Refresh() {
	canvas.Refresh(r.xAxis)
	canvas.Refresh(r.yAxis)
	canvas.Refresh(r.xAxisLabel)
	canvas.Refresh(r.yAxisLabel)

	// Create lines between points
	r.lines = nil
	for i := 0; i < len(r.plot.xData)-1; i++ {
		line := canvas.NewLine(color.RGBA{R: 0, G: 0, B: 255, A: 255})
		r.lines = append(r.lines, line)
	}

	// Create x-axis scale
	r.xScale = nil
	for i := 0; i < len(r.plot.xData); i++ {
		label := canvas.NewText(strconv.Itoa(i), color.Black)
		r.xScale = append(r.xScale, label)
	}

	// Create y-axis scale
	r.yScale = nil
	for i := 0; i <= 10; i++ {
		label := canvas.NewText(strconv.Itoa(i*10), color.Black)
		r.yScale = append(r.yScale, label)
	}

	// Create points
	r.points = nil
	for i := 0; i < len(r.plot.xData); i++ {
		point := canvas.NewCircle(color.RGBA{R: 255, G: 0, B: 0, A: 255}) // Red color for points
		point.Resize(fyne.NewSize(6, 6))
		r.points = append(r.points, point)
	}

	// Create horizontal gridlines
	r.gridlines = nil
	for i := 0; i <= 10; i++ {
		gridline := canvas.NewLine(color.RGBA{R: 220, G: 220, B: 220, A: 255}) // Light gray color for gridlines
		r.gridlines = append(r.gridlines, gridline)
	}

	// Refresh lines, scales, points, and gridlines
	for _, line := range r.lines {
		canvas.Refresh(line)
	}
	for _, label := range r.xScale {
		canvas.Refresh(label)
	}
	for _, label := range r.yScale {
		canvas.Refresh(label)
	}
	for _, point := range r.points {
		canvas.Refresh(point)
	}
	for _, gridline := range r.gridlines {
		canvas.Refresh(gridline)
	}
}

// Objects implements fyne.WidgetRenderer
func (r *plotRenderer) Objects() []fyne.CanvasObject {
	objects := []fyne.CanvasObject{r.xAxis, r.yAxis, r.xAxisLabel, r.yAxisLabel}
	objects = append(objects, r.lines...)
	objects = append(objects, r.xScale...)
	objects = append(objects, r.yScale...)
	objects = append(objects, r.points...) // Add points to objects
	objects = append(objects, r.gridlines...) // Add gridlines to objects
	return objects
}

// Destroy implements fyne.WidgetRenderer
func (r *plotRenderer) Destroy() {}

// ShowPlot initializes and displays the plot in a new window
func ShowPlot(xData, yData []float64, xAxisLabel, yAxisLabel, title string, dataChan <-chan [2]float64) {
	plotApp := app.New()
	plotApp.Settings().SetTheme(theme.LightTheme())

	if title == "" {
		title = yAxisLabel + " Plot"
	}
	plotWindow := plotApp.NewWindow(title)

	plot := NewPlot(xAxisLabel, yAxisLabel)
	for i := range yData {
		plot.UpdateData(xData[i], yData[i])
	}

	plotWindow.SetContent(container.NewVBox(
		plot,
	))

	// Goroutine to update the plot with new data points
	go func() {
		if dataChan != nil {
			for newData := range dataChan {
				plot.UpdateData(newData[0], newData[1])
			}
		}
	}()

	plotWindow.ShowAndRun()
}