package viz

import (
	"github.com/mattn/go-gtk/gtk"
)

// BaseWindow represents the main window for the visualization.
type BaseWindow struct {
	Window *gtk.Window
	Container *gtk.VBox
}

// NewBaseWindow initializes a new BaseWindow.
func NewBaseWindow(title string, width, height int) *BaseWindow {
	gtk.Init(nil)

	window := gtk.NewWindow(gtk.WINDOW_TOPLEVEL)
	window.SetTitle(title)
	window.SetDefaultSize(width, height)
	window.Connect("destroy", func() {
		gtk.MainQuit()
	})

	container := gtk.NewVBox(false, 1)
	window.Add(container)

	return &BaseWindow{
		Window: window,
		Container: container,
	}
}

// Run starts the GTK main loop.
func (bw *BaseWindow) Run() {
	bw.Window.ShowAll()
	gtk.Main()
}