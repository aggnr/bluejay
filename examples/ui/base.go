package main

import (
	"github.com/aggnr/bluejay/viz"
)

func main() {
	baseWindow := viz.NewBaseWindow("Visualization Window", 800, 600)
	baseWindow.Run()
}