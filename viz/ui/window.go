package ui

import (
	"github.com/go-gl/gl/v4.1-core/gl"
	"github.com/go-gl/glfw/v3.3/glfw"
	"log"
	"runtime"
)

type Window struct {
	Width  int
	Height int
	Title  string
	window *glfw.Window
}

func init() {
	runtime.LockOSThread()
}

func NewWindow(width, height int, title string) *Window {
	return &Window{
		Width:  width,
		Height: height,
		Title:  title,
	}
}

func (w *Window) Init() {
	if err := glfw.Init(); err != nil {
		log.Fatalln("failed to initialize glfw:", err)
	}

	glfw.WindowHint(glfw.Resizable, glfw.True)
	glfw.WindowHint(glfw.ContextVersionMajor, 4)
	glfw.WindowHint(glfw.ContextVersionMinor, 1)
	glfw.WindowHint(glfw.OpenGLProfile, glfw.OpenGLCoreProfile)
	glfw.WindowHint(glfw.OpenGLForwardCompatible, glfw.True)

	var err error
	w.window, err = glfw.CreateWindow(w.Width, w.Height, w.Title, nil, nil)
	if err != nil {
		log.Fatalln("failed to create window:", err)
	}
	w.window.MakeContextCurrent()

	if err := gl.Init(); err != nil {
		log.Fatalln("failed to initialize glow:", err)
	}
}

func (w *Window) MainLoop(draw func()) {
	for !w.window.ShouldClose() {
		gl.Clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT)
		draw()
		w.window.SwapBuffers()
		glfw.PollEvents()
	}
}

func (w *Window) Destroy() {
	w.window.Destroy()
	glfw.Terminate()
}