package publish

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/thezmc/spexma/internal/common/display"
)

// DisplayProgress shows the publishing progress in the terminal
func DisplayProgress(progress *Progress, done <-chan struct{}) {
	// Set up styles
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFF88")).MarginBottom(1)
	headerStyle := lipgloss.NewStyle().Bold(true).Underline(true).Foreground(lipgloss.Color("#FFFFFF"))
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#88AAFF"))
	valueStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#88FF88"))
	progressStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#FFAA44"))

	// Create a spinner
	nextSpinner := display.CreateSpinner(nil)

	ticker := time.NewTicker(display.DefaultUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Get progress stats
			processedFiles, totalFiles, publishedEvents, totalEvents, currentFile, elapsed, status := progress.GetStats()

			// Calculate percentages
			filePercentage := 0.0
			if totalFiles > 0 {
				filePercentage = float64(processedFiles) / float64(totalFiles) * 100
			}

			eventPercentage := 0.0
			if totalEvents > 0 {
				eventPercentage = float64(publishedEvents) / float64(totalEvents) * 100
			}

			// Get spinner frame
			spinnerFrame := nextSpinner()

			// Clear screen and show progress
			display.ClearScreen()

			// Show title
			fmt.Println(titleStyle.Render("Splunk Export Massager - Publishing to HEC"))

			// Show progress information
			fmt.Println(headerStyle.Render("Progress"))
			fmt.Printf("%s %s\n", labelStyle.Render("Status:"), valueStyle.Render(status))
			fmt.Printf("%s %s\n", labelStyle.Render("Elapsed Time:"), valueStyle.Render(elapsed.Round(time.Second).String()))
			fmt.Printf("%s %s%s\n", labelStyle.Render("Files:"), valueStyle.Render(fmt.Sprintf("%d/%d (%.1f%%)", processedFiles, totalFiles, filePercentage)), spinnerFrame)
			fmt.Printf("%s %s\n", labelStyle.Render("Events:"), valueStyle.Render(fmt.Sprintf("%d/%d (%.1f%%)", publishedEvents, totalEvents, eventPercentage)))

			if currentFile != "" {
				fmt.Printf("%s %s\n", labelStyle.Render("Current File:"), valueStyle.Render(currentFile))
			}

			// Show progress bar for events
			if totalEvents > 0 {
				width, _ := display.GetTerminalSize()
				barWidth := width - 20 // Leave room for text
				if barWidth > 100 {
					barWidth = 100 // Cap at 100 characters
				} else if barWidth < 20 {
					barWidth = 20 // Minimum size
				}

				completedWidth := int(float64(barWidth) * float64(publishedEvents) / float64(totalEvents))

				bar := fmt.Sprintf("[%s%s] %d/%d",
					progressStyle.Render(strings.Repeat("=", completedWidth)),
					strings.Repeat(" ", barWidth-completedWidth),
					publishedEvents,
					totalEvents,
				)

				fmt.Println()
				fmt.Println(bar)
			}

		case <-done:
			return
		}
	}
}

