package main

import (
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"math/rand"
	"time"
"github.com/aggnr/bluejay/viz"
)

func main() {
myApp := app.New()
myWindow := myApp.NewWindow("Live Plot")

plot := NewPlot()
myWindow.SetContent(container.NewVBox(
plot,
widget.NewButton("Update Data", func() {
plot.data = append(plot.data, rand.Float64())
plot.Refresh()
}),
))

go func() {
for range time.Tick(time.Second) {
plot.data = append(plot.data, rand.Float64())
plot.Refresh()
}
}()

myWindow.ShowAndRun()
}