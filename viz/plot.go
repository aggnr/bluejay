package viz

import (
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"fyne.io/fyne/v2/canvas"
	"image/color"
	"strconv"
)

// Plot is a custom widget to display live data as a plot
type Plot struct {
	widget.BaseWidget
	data       []float64
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
func (p *Plot) UpdateData(newData float64) {
	p.data = append(p.data, newData)
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

// plotRenderer is the renderer for the Plot widget
type plotRenderer struct {
	plot       *Plot
	xAxis      *canvas.Line
	yAxis      *canvas.Line
	xAxisLabel *canvas.Text
	yAxisLabel *canvas.Text
	lines      []fyne.CanvasObject
	xScale     []fyne.CanvasObject
	yScale     []fyne.CanvasObject
}

// Layout implements fyne.WidgetRenderer
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

	// Layout lines
	for i, line := range r.lines {
		if i < len(r.plot.data)-1 {
			x1 := padding + float32(i)*(size.Width-2*padding)/float32(len(r.plot.data)-1)
			y1 := size.Height - padding - float32(r.plot.data[i])*(size.Height-2*padding)
			x2 := padding + float32(i+1)*(size.Width-2*padding)/float32(len(r.plot.data)-1)
			y2 := size.Height - padding - float32(r.plot.data[i+1])*(size.Height-2*padding)
			line.(*canvas.Line).Position1 = fyne.NewPos(x1, y1)
			line.(*canvas.Line).Position2 = fyne.NewPos(x2, y2)
		}
	}

	// Layout x-axis scale
	for i, label := range r.xScale {
		x := padding + float32(i)*(size.Width-2*padding)/float32(len(r.plot.data)-1)
		label.Move(fyne.NewPos(x, size.Height-padding+5))
	}

	// Layout y-axis scale
	for i, label := range r.yScale {
		y := size.Height - padding - float32(i)*(size.Height-2*padding)/10
		label.Move(fyne.NewPos(padding-30, y))
	}
}

// MinSize implements fyne.WidgetRenderer
func (r *plotRenderer) MinSize() fyne.Size {
	return fyne.NewSize(800, 640)
}

// Refresh implements fyne.WidgetRenderer
func (r *plotRenderer) Refresh() {
	canvas.Refresh(r.xAxis)
	canvas.Refresh(r.yAxis)
	canvas.Refresh(r.xAxisLabel)
	canvas.Refresh(r.yAxisLabel)

	// Create lines between points
	r.lines = nil
	for i := 0; i < len(r.plot.data)-1; i++ {
		line := canvas.NewLine(color.RGBA{R: 0, G: 0, B: 255, A: 255})
		r.lines = append(r.lines, line)
	}

	// Create x-axis scale
	r.xScale = nil
	for i := 0; i < len(r.plot.data); i++ {
		label := canvas.NewText(strconv.Itoa(i), color.Black)
		r.xScale = append(r.xScale, label)
	}

	// Create y-axis scale
	r.yScale = nil
	for i := 0; i <= 10; i++ {
		label := canvas.NewText(strconv.Itoa(i*10), color.Black)
		r.yScale = append(r.yScale, label)
	}

	// Refresh lines and scales
	for _, line := range r.lines {
		canvas.Refresh(line)
	}
	for _, label := range r.xScale {
		canvas.Refresh(label)
	}
	for _, label := range r.yScale {
		canvas.Refresh(label)
	}
}

// Objects implements fyne.WidgetRenderer
func (r *plotRenderer) Objects() []fyne.CanvasObject {
	objects := []fyne.CanvasObject{r.xAxis, r.yAxis, r.xAxisLabel, r.yAxisLabel}
	objects = append(objects, r.lines...)
	objects = append(objects, r.xScale...)
	objects = append(objects, r.yScale...)
	return objects
}

// Destroy implements fyne.WidgetRenderer
func (r *plotRenderer) Destroy() {}

// ShowPlot initializes and displays the plot in a new window
func ShowPlot(xData, yData []float64, xAxisLabel, yAxisLabel string, dataChan <-chan float64) {
	plotApp := app.New()
	plotWindow := plotApp.NewWindow("Live Plot")

	plot := NewPlot(xAxisLabel, yAxisLabel)
	for _, data := range yData {
		plot.UpdateData(data)
	}

	plotWindow.SetContent(container.NewVBox(
		plot,
	))

	go func() {
		for newData := range dataChan {
			plot.UpdateData(newData)
		}
	}()

	plotWindow.ShowAndRun()
}