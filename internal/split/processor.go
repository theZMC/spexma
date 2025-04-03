package split

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

const (
	bufferSize = 1000 // Buffer size for channels
)

var splunkInternalFields = map[string]struct{}{
	"punct":     {},
	"linecount": {},
}

// ProcessCSV processes the CSV file
func ProcessCSV(inputFile, outputDir, sourcetypeCol string, stats *Stats, wg *sync.WaitGroup) error {
	// Open the input file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a buffered reader
	reader := bufio.NewReader(file)
	csvReader := csv.NewReader(reader)

	// Read the header
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("error reading header: %w", err)
	}

	// Find the index of the sourcetype column
	sourcetypeIdx := -1
	for i, column := range header {
		if column == sourcetypeCol {
			sourcetypeIdx = i
			break
		}
	}
	if sourcetypeIdx == -1 {
		return fmt.Errorf("sourcetype column '%s' not found in header", sourcetypeCol)
	}

	// Create a map to store the channels for each sourcetype
	recordChannels := make(map[string]chan []string)

	// Create a map to track non-empty columns for each sourcetype
	columnUsage := make(map[string]map[int]bool)

	// Create a channel to collect errors from writer goroutines
	errorChan := make(chan error, 100)

	// Start a goroutine to collect errors
	errorCollected := make(chan struct{})
	var processingErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errorCollected)

		for err := range errorChan {
			if processingErr == nil { // Keep only the first error
				processingErr = err
			}
		}
	}()

	// First pass: analyze CSV to determine which columns are used for each sourcetype
	fmt.Println("Pass 1: Analyzing column usage by sourcetype...")
	stats.SetProcessingPhase("analyzing")

	// Reset the file for the first pass
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("error resetting file: %w", err)
	}
	reader = bufio.NewReader(file)
	csvReader = csv.NewReader(reader)

	// Skip header
	if _, err := csvReader.Read(); err != nil {
		return fmt.Errorf("error re-reading header: %w", err)
	}

	// Analyze each record for column usage
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Warning: Error reading record during analysis: %v\n", err)
			continue
		}

		stats.IncrementAnalyzedRecords()

		// Skip records that don't have enough fields
		if len(record) <= sourcetypeIdx {
			continue
		}

		// Get the sourcetype
		sourcetype := record[sourcetypeIdx]
		if sourcetype == "" {
			sourcetype = "unknown"
		}

		// Initialize column usage map for this sourcetype if needed
		if _, exists := columnUsage[sourcetype]; !exists {
			columnUsage[sourcetype] = make(map[int]bool)
		}

		// Mark columns that are non-empty
		for i, value := range record {
			if value != "" {
				columnUsage[sourcetype][i] = true
			}
		}
	}

	// Create sourcetype-specific header maps
	sourcetypeHeaders := make(map[string][]string)
	sourcetypeHeaderIdx := make(map[string][]int)

	for sourcetype, usedColumns := range columnUsage {
		var headerList []string
		var idxList []int

		for i, colName := range header {
			if usedColumns[i] {
				headerList = append(headerList, colName)
				idxList = append(idxList, i)
			}
		}

		sourcetypeHeaders[sourcetype] = headerList
		sourcetypeHeaderIdx[sourcetype] = idxList
	}

	// Reset the file for the second pass
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("error resetting file: %w", err)
	}
	reader = bufio.NewReader(file)
	csvReader = csv.NewReader(reader)

	// Skip header again
	if _, err := csvReader.Read(); err != nil {
		return fmt.Errorf("error re-reading header: %w", err)
	}

	fmt.Println("Pass 2: Processing records...")
	stats.SetProcessingPhase("processing")

	// Process each record
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Warning: Error reading record: %v\n", err)
			continue
		}

		// Skip records that don't have enough fields
		if len(record) <= sourcetypeIdx {
			fmt.Printf("Warning: Record has insufficient fields: %v\n", record)
			continue
		}

		// Get the sourcetype
		sourcetype := record[sourcetypeIdx]
		if sourcetype == "" {
			sourcetype = "unknown"
		}

		// Create a new channel and writer for this sourcetype if it doesn't exist
		if _, exists := recordChannels[sourcetype]; !exists {
			recordChannels[sourcetype] = make(chan []string, bufferSize)

			// Create the output file with sanitized filename
			sanitizedSourcetype := sanitizeFilename(sourcetype)
			outputFile := filepath.Join(outputDir, sanitizedSourcetype+".csv")

			// Start a goroutine to write to this file
			wg.Add(1)
			go func(st string, ch chan []string, headerCols []string, colIndices []int) {
				defer wg.Done()

				if err := writeCSV(outputFile, headerCols, ch, stats, st, colIndices); err != nil {
					errorChan <- fmt.Errorf("error writing to %s: %w", outputFile, err)
				}
			}(sourcetype, recordChannels[sourcetype], sourcetypeHeaders[sourcetype], sourcetypeHeaderIdx[sourcetype])
		}

		// Send the record to the appropriate channel
		select {
		case recordChannels[sourcetype] <- record:
			// Record sent successfully
		default:
			// Channel buffer is full, write synchronously
			recordChannels[sourcetype] <- record
		}
	}

	// Close all channels to signal writers to finish
	for _, ch := range recordChannels {
		close(ch)
	}

	// Close the error channel once all writers are done
	go func() {
		// We need a separate WaitGroup to avoid deadlock with the main one
		var writerWg sync.WaitGroup
		writerWg.Add(len(recordChannels))

		for range recordChannels {
			writerWg.Done()
		}

		writerWg.Wait()
		close(errorChan)
	}()

	// Wait for error collection to finish
	<-errorCollected

	return processingErr
}

// Write records to a CSV file with filtered columns
func writeCSV(filename string, header []string, records <-chan []string, stats *Stats,
	sourcetype string, colIndices []int,
) error {
	// Create the output file (truncates if it exists)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	// Create a buffered writer
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Create a CSV writer
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// Write the filtered header
	if err := csvWriter.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write each record with only the relevant columns
	for record := range records {
		// Filter the record to include only the specified columns
		var filteredRecord []string
		for _, idx := range colIndices {
			if idx < len(record) {
				filteredRecord = append(filteredRecord, record[idx])
			} else {
				filteredRecord = append(filteredRecord, "")
			}
		}

		if err := csvWriter.Write(filteredRecord); err != nil {
			return fmt.Errorf("error writing record: %w", err)
		}

		stats.IncrementRecord(sourcetype)

		// Flush periodically to ensure data is written
		if _, countsMap := stats.GetStats(); countsMap[sourcetype]%1000 == 0 {
			csvWriter.Flush()
			writer.Flush()
		}
	}

	return nil
}

// Sanitize a sourcetype string to create a valid filename
func sanitizeFilename(name string) string {
	// Replace characters that are illegal in filenames on various operating systems
	// with underscores
	re := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	sanitized := re.ReplaceAllString(name, "_")

	// Replace multiple consecutive underscores with a single one
	sanitized = regexp.MustCompile(`_+`).ReplaceAllString(sanitized, "_")

	// Remove leading and trailing spaces/dots/underscores
	sanitized = strings.Trim(sanitized, " ._")

	// If the name becomes empty after sanitization, use a default
	if sanitized == "" {
		sanitized = "unknown"
	}

	return sanitized
}
