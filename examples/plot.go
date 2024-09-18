//go:build ignoreme
// +build ignoreme

package main

import (
	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/ebitenutil"
	"image/color"
	"log"
)

// Game holds the correlation matrix and other necessary data.
type Game struct {
	matrix [][]float64
	cellSize int
}

// NewGame initializes a new Game instance.
func NewGame(matrix [][]float64) *Game {
	return &Game{
		matrix: matrix,
		cellSize: 50,
	}
}

// Update updates the game state.
func (g *Game) Update() error {
	return nil
}

// Draw draws the correlation matrix on the screen.
func (g *Game) Draw(screen *ebiten.Image) {
	for i := range g.matrix {
		for j := range g.matrix[i] {
			corr := g.matrix[i][j]
			col := correlationToColor(corr)
			x := i * g.cellSize
			y := j * g.cellSize
			ebitenutil.DrawRect(screen, float64(x), float64(y), float64(g.cellSize), float64(g.cellSize), col)
		}
	}
}

// Layout sets the screen layout.
func (g *Game) Layout(outsideWidth, outsideHeight int) (int, int) {
	width := len(g.matrix) * g.cellSize
	height := len(g.matrix) * g.cellSize
	return width, height
}

// correlationToColor maps a correlation value to a color.
func correlationToColor(corr float64) color.Color {
	norm := (corr + 1) / 2
	r := uint8(255 * norm)
	b := uint8(255 * (1 - norm))
	return color.RGBA{R: r, G: 0, B: b, A: 255}
}

func main() {
	matrix := [][]float64{
		{1, 0.8, 0.6},
		{0.8, 1, 0.4},
		{0.6, 0.4, 1},
	}

	game := NewGame(matrix)
	ebiten.SetWindowSize(len(matrix)*50, len(matrix)*50)
	ebiten.SetWindowTitle("Correlation Matrix")

	if err := ebiten.RunGame(game); err != nil {
		log.Fatal(err)
	}
}