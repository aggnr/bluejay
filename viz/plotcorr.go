package bluejay

import (
	"fmt"
	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/ebitenutil"
	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"image/color"
	"log"
	"os"
)

var (
	fontFace font.Face
)

// game holds the correlation matrix and other necessary data.
type game struct {
	matrix   [][]float64
	columns  []string
	cellSize int
}

// newGame initializes a new game instance.
func newGame(matrix [][]float64, columns []string) *game {
	return &game{
		matrix:   matrix,
		columns:  columns,
		cellSize: 50,
	}
}

// loadFont loads the "Source Code Pro" font from the viz/fonts directory.
func loadFont() {
	fontBytes, err := os.ReadFile("viz/fonts/SourceCodePro-Regular.ttf")
	if err != nil {
		log.Fatalf("failed to read font file: %v", err)
	}
	font, err := opentype.Parse(fontBytes)
	if err != nil {
		log.Fatalf("failed to parse font: %v", err)
	}
	const dpi = 72
	fontFace, err = opentype.NewFace(font, &opentype.FaceOptions{
		Size: 16,
		DPI:  dpi,
	})
	if err != nil {
		log.Fatalf("failed to create font face: %v", err)
	}
}


// Update updates the game state.
func (g *game) Update() error {
	return nil
}

func (g *game) Draw(screen *ebiten.Image) {
	// Set a dark grey background color
	screen.Fill(color.RGBA{R: 100, G: 100, B: 100, A: 255})

	for i := range g.matrix {
		for j := range g.matrix[i] {
			corr := g.matrix[i][j]
			col := correlationToColor(corr)
			x := (i + 1) * g.cellSize + 40 // Add consistent border space
			y := (j + 1) * g.cellSize + 40 // Add consistent border space
			ebitenutil.DrawRect(screen, float64(x), float64(y), float64(g.cellSize), float64(g.cellSize), col)

			// Draw borders
			borderColor := color.RGBA{0, 0, 0, 255} // Black color for borders
			ebitenutil.DrawLine(screen, float64(x), float64(y), float64(x+g.cellSize), float64(y), borderColor) // Top border
			ebitenutil.DrawLine(screen, float64(x), float64(y), float64(x), float64(y+g.cellSize), borderColor) // Left border
			ebitenutil.DrawLine(screen, float64(x+g.cellSize), float64(y), float64(x+g.cellSize), float64(y+g.cellSize), borderColor) // Right border
			ebitenutil.DrawLine(screen, float64(x), float64(y+g.cellSize), float64(x+g.cellSize), float64(y+g.cellSize), borderColor) // Bottom border

			// Draw correlation value
			text := fmt.Sprintf("%.2f", corr)
			ebitenutil.DebugPrintAt(screen, text, x+g.cellSize/4, y+g.cellSize/4)
		}
	}

	// Draw column names
	for i, colName := range g.columns {
		ebitenutil.DebugPrintAt(screen, colName, (i+1)*g.cellSize+g.cellSize/4+40, 10) // Add more border space
		ebitenutil.DebugPrintAt(screen, colName, 10, (i+1)*g.cellSize+g.cellSize/4+40) // Add more border space
	}

	// Draw heatmap legend
	legendX := len(g.matrix)*g.cellSize + 100 // Move legend to the right
	legendY := 40 // Align legend with the top of the matrix

	for i := 0; i <= 100; i++ {
		col := correlationToColor(float64(i)/50 - 1)
		ebitenutil.DrawRect(screen, float64(legendX), float64(legendY+i), 20, 1, col) // Adjust position for border space
	}
	ebitenutil.DebugPrintAt(screen, "Legend", legendX, legendY + 110)
	ebitenutil.DebugPrintAt(screen, "-1", legendX + 25, legendY)
	ebitenutil.DebugPrintAt(screen, "0", legendX + 25, legendY + 50)
	ebitenutil.DebugPrintAt(screen, "1", legendX + 25, legendY + 100)
}

// Layout sets the screen layout.
func (g *game) Layout(outsideWidth, outsideHeight int) (int, int) {
	width := (len(g.matrix) + 1) * g.cellSize + 80 // Add consistent border space
	height := (len(g.matrix[0]) + 1) * g.cellSize + 80 // Add consistent border space
	return width + 60, height
}

// seabornColorPalette maps a normalized value to a color using a Seaborn-like palette.
func seabornColorPalette(value float64) color.Color {
	// Ensure the value is between 0 and 1
	if value < 0 {
		value = 0
	} else if value > 1 {
		value = 1
	}

	// Define the colors for the gradient
	blue := color.RGBA{R: 68, G: 119, B: 170, A: 255}
	white := color.RGBA{R: 255, G: 255, B: 255, A: 255}
	red := color.RGBA{R: 204, G: 102, B: 119, A: 255}

	// Interpolate between blue and white for values [0, 0.5]
	if value <= 0.5 {
		r := uint8(float64(blue.R) + value*2*float64(white.R-blue.R))
		g := uint8(float64(blue.G) + value*2*float64(white.G-blue.G))
		b := uint8(float64(blue.B) + value*2*float64(white.B-blue.B))
		return color.RGBA{R: r, G: g, B: b, A: 255}
	}

	// Interpolate between white and red for values (0.5, 1]
	value = (value - 0.5) * 2
	r := uint8(float64(white.R) + value*float64(red.R-white.R))
	g := uint8(float64(white.G) + value*float64(red.G-white.G))
	b := uint8(float64(white.B) + value*float64(red.B-white.B))
	return color.RGBA{R: r, G: g, B: b, A: 255}
}

// correlationToColor maps a correlation value to a color using the Seaborn-like palette.
func correlationToColor(corr float64) color.Color {
	// Normalize the correlation value to [0, 1]
	norm := (corr + 1) / 2
	return seabornColorPalette(norm)
}

// PlotCorrMat plots the given correlation matrix using ebiten.
func PlotCorrMat(matrix [][]float64, columns []string) {
	loadFont() // Load the font before starting the game
	game := newGame(matrix, columns)
	windowWidth := (len(matrix) + 1) * 50 + 140 // Adjust window size for border space
	windowHeight := (len(matrix[0]) + 1) * 50 + 140 // Adjust window size for border space
	ebiten.SetWindowSize(windowWidth, windowHeight)
	ebiten.SetWindowTitle("Correlation Matrix")

	if err := ebiten.RunGame(game); err != nil {
		log.Fatal(err)
	}
}