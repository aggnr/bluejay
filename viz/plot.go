package viz

import (
"fyne.io/fyne/v2"
"fyne.io/fyne/v2/canvas"
"fyne.io/fyne/v2/widget"
"image/color"
)

// Plot is a custom widget to display live data as a plot
type Plot struct {
	widget.BaseWidget
	data []float64
}

// NewPlot creates a new Plot widget
func NewPlot() *Plot {
	plot := &Plot{}
	plot.ExtendBaseWidget(plot)
	return plot
}

// CreateRenderer implements fyne.WidgetRenderer
func (p *Plot) CreateRenderer() fyne.WidgetRenderer {
	line := canvas.NewLine(color.RGBA{R: 0, G: 0, B: 255, A: 255})
	return &plotRenderer{plot: p, line: line}
}

// plotRenderer is the renderer for the Plot widget
type plotRenderer struct {
	plot *Plot
	line *canvas.Line
}

// Layout implements fyne.WidgetRenderer
func (r *plotRenderer) Layout(size fyne.Size) {
	r.line.Position1 = fyne.NewPos(0, size.Height/2)
	r.line.Position2 = fyne.NewPos(size.Width, size.Height/2)
}

// MinSize implements fyne.WidgetRenderer
func (r *plotRenderer) MinSize() fyne.Size {
	return fyne.NewSize(100, 100)
}

// Refresh implements fyne.WidgetRenderer
func (r *plotRenderer) Refresh() {
	r.line.StrokeColor = color.RGBA{R: 0, G: 0, B: 255, A: 255}
	canvas.Refresh(r.line)
}

// Objects implements fyne.WidgetRenderer
func (r *plotRenderer) Objects() []fyne.CanvasObject {
	return []fyne.CanvasObject{r.line}
}

// Destroy implements fyne.WidgetRenderer
func (r *plotRenderer) Destroy() {}


