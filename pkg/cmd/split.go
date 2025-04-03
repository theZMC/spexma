package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/cobra"
	"github.com/thezmc/spexma/internal/split"
)

// Options for the split command
var (
	inputFile       string
	outputDirectory string
	sourcetypeCol   string = "sourcetype" // Default column name
)

// splitCmd represents the split command
var splitCmd = &cobra.Command{
	Use:   "split",
	Short: "Split a Splunk CSV export by sourcetype",
	Long: `Split a Splunk CSV export into multiple CSV files, one for each sourcetype.
Each output file will only contain columns that are relevant for that sourcetype.

Example:
  spexma split -i export.csv -o ./output_dir`,
	Run: runSplit,
}

func init() {
	// Define flags for the split command
	splitCmd.Flags().StringVarP(&inputFile, "input-file", "i", "", "Input CSV file (required)")
	splitCmd.Flags().StringVarP(&outputDirectory, "output-directory", "o", "", "Output directory for the split CSV files (defaults to same directory as input)")
	splitCmd.Flags().StringVarP(&sourcetypeCol, "column", "c", sourcetypeCol, "Column name for sourcetype")

	// Mark required flags
	splitCmd.MarkFlagRequired("input-file")
}

func runSplit(cmd *cobra.Command, args []string) {
	// Validate input file
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		fmt.Printf("Error: Input file '%s' does not exist\n", inputFile)
		os.Exit(1)
	}

	// If output directory is not provided, use the same directory as the input file
	if outputDirectory == "" {
		outputDirectory = filepath.Dir(inputFile)
	} else {
		// Ensure output directory exists
		if _, err := os.Stat(outputDirectory); os.IsNotExist(err) {
			fmt.Printf("Creating output directory: %s\n", outputDirectory)
			if err := os.MkdirAll(outputDirectory, 0o755); err != nil {
				fmt.Printf("Error creating output directory '%s': %v\n", outputDirectory, err)
				os.Exit(1)
			}
		}
	}

	// Create stats tracker
	statsTracker := split.NewStats()

	// Set initial processing phase
	statsTracker.SetProcessingPhase("initializing")

	// Set up a wait group for goroutines
	var wg sync.WaitGroup

	// Start the display updater
	displayDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		split.UpdateDisplay(statsTracker, displayDone)
	}()

	// Process the CSV
	fmt.Println("Processing CSV file:", inputFile)
	fmt.Println("Output directory:", outputDirectory)
	fmt.Println("This will overwrite any existing CSV files with the same sourcetype names.")
	err := split.ProcessCSV(inputFile, outputDirectory, sourcetypeCol, statsTracker, &wg)

	// Signal display updater to stop
	close(displayDone)

	// Wait for all goroutines to finish
	wg.Wait()

	// Print final stats
	order, records := statsTracker.GetStats()
	fmt.Println("\nProcessing completed.")
	fmt.Println("Summary:")
	fmt.Println("--------------------")
	totalRecords := 0
	for _, st := range order {
		fmt.Printf("%-30s: %d records\n", st, records[st])
		totalRecords += records[st]
	}
	fmt.Println("--------------------")
	fmt.Printf("Total: %d records processed\n", totalRecords)

	// Check for errors
	if err != nil {
		fmt.Printf("\nError during processing: %v\n", err)
		os.Exit(1)
	}
}
