package ui

import (
	"github.com/go-gl/gl/v4.1-core/gl"
	"github.com/go-gl/glfw/v3.3/glfw"
	"log"
	"runtime"
)

const (
	uiWidth  = 300
	uiHeight = 200
)

func init() {
	runtime.LockOSThread()
}

func Run() {
	if err := glfw.Init(); err != nil {
		log.Fatalln("failed to initialize glfw:", err)
	}
	defer glfw.Terminate()

	glfw.WindowHint(glfw.Resizable, glfw.False)
	glfw.WindowHint(glfw.ContextVersionMajor, 4)
	glfw.WindowHint(glfw.ContextVersionMinor, 1)
	glfw.WindowHint(glfw.OpenGLProfile, glfw.OpenGLCoreProfile)
	glfw.WindowHint(glfw.OpenGLForwardCompatible, glfw.True)

	window, err := glfw.CreateWindow(uiWidth, uiHeight, "UI Example", nil, nil)
	if err != nil {
		log.Fatalln("failed to create window:", err)
	}
	window.MakeContextCurrent()

	if err := gl.Init(); err != nil {
		log.Fatalln("failed to initialize glow:", err)
	}

	for !window.ShouldClose() {
		// Clear the screen to white
		gl.ClearColor(1.0, 1.0, 1.0, 1.0)
		gl.Clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT)

		// Draw UI elements here

		// Swap buffers
		window.SwapBuffers()
		glfw.PollEvents()
	}
}