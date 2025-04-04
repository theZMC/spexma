package cmd

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/thezmc/spexma/internal/publish"
	"github.com/thezmc/spexma/internal/publish/hec"
)

// Options for the publish command
var (
	// Input options
	inputDirectory  string
	timeField       string = "_time"
	timeFormat      string = "epoch"
	sourcetypeField string = "sourcetype"

	// HEC options
	hecURL       string
	hecToken     string
	hecInsecure  bool
	hecBatchSize int           = 100
	hecTimeout   time.Duration = 30 * time.Second

	// Output options
	indexName   string
	hostValue   string
	sourceValue string

	// Processing options
	concurrency   int           = 4
	retryCount    int           = 3
	retryWait     time.Duration = 2 * time.Second
	dryRun        bool
	debugMode     bool
	timeOffset    time.Duration = 0
	excludeFields []string      = []string{
		"date_hour",
		"date_mday",
		"date_minute",
		"date_month",
		"date_wday",
		"date_year",
		"date_zone",
	}
)

// publishCmd represents the publish command
var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish CSV data to Splunk HEC",
	Long: `Publish CSV data to Splunk HTTP Event Collector (HEC).
This command reads CSV files and sends the data as events to a Splunk instance.

Example:
  spexma publish -i ./split_data -u https://splunk:8088/services/collector -t YOUR_HEC_TOKEN`,
	Run: runPublish,
}

func init() {
	// Define flags for the publish command

	// Input options
	publishCmd.Flags().StringVarP(&inputDirectory, "input-directory", "i", "", "Input directory containing CSV files (required)")
	publishCmd.Flags().StringVar(&timeField, "time-field", timeField, "Field containing the timestamp")
	publishCmd.Flags().StringVar(&timeFormat, "time-format", timeFormat, "Format for parsing timestamps (epoch, epoch_ms, or a go time format string)")
	publishCmd.Flags().StringVar(&sourcetypeField, "sourcetype-field", sourcetypeField, "Field containing the sourcetype")

	// HEC options
	publishCmd.Flags().StringVarP(&hecURL, "url", "u", "", "Splunk HEC URL (required)")
	publishCmd.Flags().StringVarP(&hecToken, "token", "t", "", "Splunk HEC token (required)")
	publishCmd.Flags().BoolVar(&hecInsecure, "insecure", false, "Skip TLS verification")
	publishCmd.Flags().IntVar(&hecBatchSize, "batch-size", hecBatchSize, "Number of events to send in a batch")
	publishCmd.Flags().DurationVar(&hecTimeout, "timeout", hecTimeout, "HEC request timeout")

	// Output options
	publishCmd.Flags().StringVar(&indexName, "index", "", "Splunk index to send events to")
	publishCmd.Flags().StringVar(&hostValue, "host", "", "Host value for events")
	publishCmd.Flags().StringVar(&sourceValue, "source", "", "Source value for events")

	// Processing options
	publishCmd.Flags().IntVar(&concurrency, "concurrency", concurrency, "Number of concurrent workers")
	publishCmd.Flags().IntVar(&retryCount, "retry-count", retryCount, "Number of times to retry failed requests")
	publishCmd.Flags().DurationVar(&retryWait, "retry-wait", retryWait, "Time to wait between retries")
	publishCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Don't actually send events, just show what would be sent")
	publishCmd.Flags().BoolVar(&debugMode, "debug", false, "Enable debug logging")
	publishCmd.Flags().DurationVar(&timeOffset, "time-offset", timeOffset, "Offset to apply to timestamps (e.g., -1h, +30m)")
	publishCmd.Flags().StringArrayVar(&excludeFields, "exclude-fields", excludeFields, "Fields to exclude from the event; Splunk's default date expansion fields are excluded by default")

	// Mark required flags
	publishCmd.MarkFlagRequired("input-directory")
	publishCmd.MarkFlagRequired("url")
	publishCmd.MarkFlagRequired("token")
}

func runPublish(cmd *cobra.Command, args []string) {
	// Validate input directory
	if _, err := os.Stat(inputDirectory); os.IsNotExist(err) {
		fmt.Printf("Error: Input directory '%s' does not exist\n", inputDirectory)
		os.Exit(1)
	}

	// Create HEC client
	hecOptions := &hec.Options{
		InsecureSSL:   hecInsecure,
		Timeout:       hecTimeout,
		BatchSize:     hecBatchSize,
		DefaultIndex:  indexName,
		DefaultHost:   hostValue,
		DefaultSource: sourceValue,
		Debug:         debugMode,
	}
	hecClient := hec.NewClient(hecURL, hecToken, hecOptions)

	// Print debug configuration if enabled
	if debugMode {
		fmt.Println("Debug mode enabled - detailed logs will be printed")
		fmt.Printf("HEC URL: %s\n", hecURL)
		fmt.Printf("Index: %s\n", indexName)
		fmt.Printf("Host: %s\n", hostValue)
		fmt.Printf("Source: %s\n", sourceValue)
		fmt.Printf("Batch size: %d\n", hecBatchSize)
		fmt.Printf("Time field: %s\n", timeField)
		fmt.Printf("Time format: %s\n", timeFormat)
	}

	// Verify HEC connection (unless in dry run mode)
	if !dryRun {
		fmt.Println("Testing connection to Splunk HEC...")
		if err := hecClient.HealthCheck(); err != nil {
			fmt.Printf("Error connecting to Splunk HEC: %v\n", err)
			fmt.Println("If you're testing, you can use --dry-run to skip the connection check.")
			os.Exit(1)
		}
		fmt.Println("Connection successful!")
	}

	// Create transformer configuration
	tConfig := &publish.TransformerConfig{
		ExcludeFields:    []string{},
		TimeField:        timeField,
		TimeFormat:       timeFormat,
		Host:             hostValue,
		Source:           sourceValue,
		Index:            indexName,
		PreserveNulls:    false,
		StrictMapping:    false,
		DefaultTimestamp: nil, // Use current time as default if needed
		TimeOffset:       timeOffset,
	}

	// If the current hostname should be used
	if hostValue == "auto" || hostValue == "" {
		hostname, err := os.Hostname()
		if err == nil {
			tConfig.Host = hostname
		}
	}

	// Create transformer
	t := publish.NewTransformer(tConfig)

	// Create progress channel
	progressCh := make(chan *publish.Progress, 10)

	// Create publisher configuration
	pConfig := &publish.PublisherConfig{
		HECClient:       hecClient,
		Transformer:     t,
		Concurrency:     concurrency,
		BatchSize:       hecBatchSize,
		RetryCount:      retryCount,
		RetryWait:       retryWait,
		OutputDirectory: "", // Not used for publish
		ProgressCh:      progressCh,
		DryRun:          dryRun,
		Debug:           debugMode,
	}

	// Create publisher
	p := publish.NewPublisher(pConfig)

	// Set up display
	displayDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		publish.DisplayProgress(p.GetProgress(), displayDone)
	}()

	// Start publishing
	fmt.Printf("Publishing CSV data from '%s' to Splunk HEC at '%s'\n", inputDirectory, hecURL)
	if dryRun {
		fmt.Println("DRY RUN MODE: No events will actually be sent to Splunk")
	}

	err := p.PublishDirectory(inputDirectory)

	// Signal display to stop
	close(displayDone)

	// Wait for display to finish
	wg.Wait()

	// Check for errors
	if err != nil {
		fmt.Printf("\nError during publishing: %v\n", err)
		os.Exit(1)
	}

	// Get final stats
	processedFiles, totalFiles, publishedEvents, totalEvents, _, elapsed, _ := p.GetProgress().GetStats()

	// Print summary
	fmt.Println("\nPublishing completed!")
	fmt.Println("Summary:")
	fmt.Println("--------------------")
	fmt.Printf("Files processed: %d/%d\n", processedFiles, totalFiles)
	fmt.Printf("Events published: %d/%d\n", publishedEvents, totalEvents)
	fmt.Printf("Total time: %s\n", elapsed.Round(time.Second))
	fmt.Printf("Average rate: %.2f events/second\n", float64(publishedEvents)/elapsed.Seconds())

	// Show additional info for dry run
	if dryRun {
		fmt.Println("\nThis was a dry run. No events were actually sent to Splunk.")
		fmt.Println("Remove the --dry-run flag to publish events for real.")
	}
}
