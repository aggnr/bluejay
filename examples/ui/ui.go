package main

import (
	"github.com/aggnr/bluejay/viz/ui"
)

func Run() {
	window := NewWindow(800, 600, "Resizable Window Example")
	window.Init()
	defer window.Destroy()

	window.MainLoop(func() {

		// Draw UI elements here
	})
}

func main() {

	Run()
}