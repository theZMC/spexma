package split

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/thezmc/spexma/internal/common/display"
)

const (
	minWidth        = 20 // Minimum width for columns
	defaultStWidth  = 40 // Default sourcetype column width
	defaultRecWidth = 15 // Default records column width
)

// UpdateDisplay updates the terminal display with current stats
func UpdateDisplay(stats *Stats, done <-chan struct{}) {
	// Set up styles
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFF88")).MarginBottom(1)
	headerStyle := lipgloss.NewStyle().Bold(true).Underline(true).Foreground(lipgloss.Color("#FFFFFF"))
	sourceTypeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#88AAFF"))
	recordsStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#88FF88"))
	phaseStyle := lipgloss.NewStyle().Italic(true).Foreground(lipgloss.Color("#FFAA44"))

	// Create a spinner
	nextSpinner := display.CreateSpinner(nil)

	ticker := time.NewTicker(display.DefaultUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Get the current phase and stats
			phase := stats.GetProcessingPhase()
			order, records := stats.GetStats()

			// Calculate optimal column widths
			terminalWidth, _ := display.GetTerminalSize()
			maxStLen := stats.GetMaxSourcetypeLen()

			// Ensure sourcetype column width is at least as wide as the longest sourcetype
			stWidth := int(math.Max(float64(maxStLen+2), float64(minWidth)))

			// Adjust width if terminal is too small
			availableWidth := terminalWidth - 3 // Account for spaces between columns

			// If sourcetype width is too large, cap it
			if stWidth > availableWidth-defaultRecWidth {
				stWidth = availableWidth - defaultRecWidth
				if stWidth < minWidth {
					stWidth = minWidth
				}
			}

			recWidth := availableWidth - stWidth
			if recWidth < minWidth {
				recWidth = minWidth
			}

			// Clear the screen
			display.ClearScreen()

			// Show the title
			fmt.Println(titleStyle.Render("Splunk Export Massager (spexma)"))

			// Show current phase with spinner
			spinner := nextSpinner()
			phaseStr := fmt.Sprintf("Current phase: %s %s",
				phaseStyle.Render(phase),
				spinner)

			// Add progress info based on the phase
			if phase == "analyzing" {
				analyzed := stats.GetAnalyzedRecords()
				phaseStr += fmt.Sprintf(" (Analyzing... %d records so far)", analyzed)
			}

			fmt.Println(phaseStr)
			fmt.Println()

			// Format the header
			headerLine := fmt.Sprintf("%-*s %-*s",
				stWidth, headerStyle.Render("Sourcetype"),
				recWidth, headerStyle.Render("Records"))
			fmt.Println(headerLine)

			divider := lipgloss.NewStyle().Foreground(lipgloss.Color("#444444")).Render(
				strings.Repeat("-", stWidth+recWidth+1))
			fmt.Println(divider)

			// Show each sourcetype
			for _, st := range order {
				stDisplay := st
				if len(stDisplay) > stWidth-3 && len(stDisplay) > 3 {
					stDisplay = stDisplay[:stWidth-3] + "..."
				}

				line := fmt.Sprintf("%-*s %-*s",
					stWidth, sourceTypeStyle.Render(stDisplay),
					recWidth, recordsStyle.Render(fmt.Sprintf("%d", records[st])))
				fmt.Println(line)
			}

			// If we're in analyzing phase and no sourcetypes yet, show a message
			if phase == "analyzing" && len(order) == 0 {
				fmt.Println(lipgloss.NewStyle().Faint(true).Render("Waiting for sourcetypes to be discovered..."))
			}

			// Clear any remaining lines
			display.ClearToEndOfScreen()

		case <-done:
			return
		}
	}
}
