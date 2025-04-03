package display

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/term"
)

const (
	// Default update interval for displays
	DefaultUpdateInterval = 100 * time.Millisecond

	// Default terminal dimensions if detection fails
	DefaultTerminalWidth  = 80
	DefaultTerminalHeight = 24

	// Minimum terminal width to ensure readable display
	MinTerminalWidth = 60
)

// GetTerminalSize returns the width and height of the terminal
func GetTerminalSize() (width, height int) {
	// Try to get size using the term package (cross-platform)
	if f, err := os.Open("/dev/tty"); err == nil {
		defer f.Close()

		width, height, err := term.GetSize(int(f.Fd()))
		if err == nil {
			return width, height
		}
	}

	// Try with stdin if /dev/tty is not available
	width, height, err := term.GetSize(int(os.Stdin.Fd()))
	if err == nil {
		return width, height
	}

	// Fall back to environment variables
	if cols := os.Getenv("COLUMNS"); cols != "" {
		var width int
		if _, err := fmt.Sscanf(cols, "%d", &width); err == nil && width > 0 {
			height := DefaultTerminalHeight

			if rows := os.Getenv("LINES"); rows != "" {
				var h int
				if _, err := fmt.Sscanf(rows, "%d", &h); err == nil && h > 0 {
					height = h
				}
			}

			return width, height
		}
	}

	// Use default values if all else fails
	return DefaultTerminalWidth, DefaultTerminalHeight
}

// ClearScreen clears the terminal screen
func ClearScreen() {
	fmt.Print("\033[H\033[2J")
}

// MoveCursor moves the cursor to the specified position
func MoveCursor(row, col int) {
	fmt.Printf("\033[%d;%dH", row, col)
}

// ClearToEndOfScreen clears from the cursor to the end of screen
func ClearToEndOfScreen() {
	fmt.Print("\033[J")
}

// ClearToEndOfLine clears from the cursor to the end of line
func ClearToEndOfLine() {
	fmt.Print("\033[K")
}

// CreateSpinner returns a text-based spinner with the given frames
func CreateSpinner(frames []string) func() string {
	if len(frames) == 0 {
		frames = []string{"⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"}
	}

	idx := 0
	return func() string {
		frame := frames[idx]
		idx = (idx + 1) % len(frames)
		return frame
	}
}
