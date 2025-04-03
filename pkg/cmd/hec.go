package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/thezmc/spexma/internal/publish/hec"
)

// Options for the hec-test command
var (
	testHecURL      string
	testHecToken    string
	testHecInsecure bool
	testIndex       string
	testHost        string
	testSource      string
	testSourcetype  string
	testNumEvents   int = 5
	testVerbose     bool
)

// hecTestCmd represents the hec-test command
var hecTestCmd = &cobra.Command{
	Use:   "hec-test",
	Short: "Test Splunk HEC connectivity and event acceptance",
	Long: `Test Splunk HEC connectivity and event acceptance.
This command sends test events to a Splunk HEC endpoint to verify functionality.

Example:
  spexma hec-test -u https://splunk:8088/services/collector -t YOUR_HEC_TOKEN`,
	Run: runHecTest,
}

func init() {
	// Define flags for the command
	hecTestCmd.Flags().StringVarP(&testHecURL, "url", "u", "", "Splunk HEC URL (required)")
	hecTestCmd.Flags().StringVarP(&testHecToken, "token", "t", "", "Splunk HEC token (required)")
	hecTestCmd.Flags().BoolVar(&testHecInsecure, "insecure", false, "Skip TLS verification")
	hecTestCmd.Flags().StringVar(&testIndex, "index", "", "Splunk index to send events to")
	hecTestCmd.Flags().StringVar(&testHost, "host", "", "Host value for events")
	hecTestCmd.Flags().StringVar(&testSource, "source", "spexma:test", "Source value for events")
	hecTestCmd.Flags().StringVar(&testSourcetype, "sourcetype", "spexma:test", "Sourcetype value for events")
	hecTestCmd.Flags().IntVar(&testNumEvents, "events", testNumEvents, "Number of test events to send")
	hecTestCmd.Flags().BoolVarP(&testVerbose, "verbose", "v", false, "Enable verbose output")

	// Mark required flags
	hecTestCmd.MarkFlagRequired("url")
	hecTestCmd.MarkFlagRequired("token")

	// Add to root command
	rootCmd.AddCommand(hecTestCmd)
}

func runHecTest(cmd *cobra.Command, args []string) {
	fmt.Println("Splunk HEC Test")
	fmt.Println("===============")
	fmt.Printf("URL: %s\n", testHecURL)
	fmt.Printf("Index: %s\n", testIndex)
	fmt.Printf("Host: %s\n", testHost)
	fmt.Printf("Source: %s\n", testSource)
	fmt.Printf("Sourcetype: %s\n", testSourcetype)
	fmt.Printf("Number of events: %d\n", testNumEvents)
	fmt.Println()

	// Create HEC client
	hecOptions := &hec.Options{
		InsecureSSL:   testHecInsecure,
		Timeout:       30 * time.Second,
		BatchSize:     testNumEvents,
		DefaultIndex:  testIndex,
		DefaultHost:   testHost,
		DefaultSource: testSource,
		Debug:         testVerbose,
	}
	hecClient := hec.NewClient(testHecURL, testHecToken, hecOptions)

	// Test basic connectivity first with a single simple event
	fmt.Println("Stage 1: Testing basic connectivity...")
	testEvent := hec.Event{
		Event: map[string]interface{}{
			"message": "HEC connection test from spexma",
			"test":    "basic_connectivity",
		},
		SourceType: testSourcetype,
	}

	if err := hecClient.SendEvent(testEvent); err != nil {
		fmt.Printf("ERROR: Basic connectivity test failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("SUCCESS: Basic connectivity test passed")
	fmt.Println()

	// Test with current timestamp
	fmt.Println("Stage 2: Testing with current timestamp...")
	now := time.Now().Unix()
	timestampEvent := hec.Event{
		Time: &now,
		Event: map[string]interface{}{
			"message": "HEC timestamp test from spexma",
			"test":    "timestamp_test",
		},
		SourceType: testSourcetype,
	}

	if err := hecClient.SendEvent(timestampEvent); err != nil {
		fmt.Printf("ERROR: Timestamp test failed: %v\n", err)
		fmt.Println("NOTE: This may indicate issues with timestamp handling")
	} else {
		fmt.Println("SUCCESS: Timestamp test passed")
	}
	fmt.Println()

	// Test with multiple events in a batch
	fmt.Printf("Stage 3: Testing with a batch of %d events...\n", testNumEvents)
	var batchEvents []hec.Event
	for i := 0; i < testNumEvents; i++ {
		ts := time.Now().Add(time.Duration(i) * time.Second).Unix()
		event := hec.Event{
			Time: &ts,
			Event: map[string]interface{}{
				"message": fmt.Sprintf("HEC batch test event %d from spexma", i+1),
				"test":    "batch_test",
				"counter": i,
				"random":  fmt.Sprintf("random_value_%d", time.Now().Nanosecond()),
			},
			SourceType: testSourcetype,
		}
		batchEvents = append(batchEvents, event)
	}

	if err := hecClient.SendEvents(batchEvents); err != nil {
		fmt.Printf("ERROR: Batch test failed: %v\n", err)
		fmt.Println("NOTE: This may indicate issues with batch processing")
	} else {
		fmt.Println("SUCCESS: Batch test passed")
	}
	fmt.Println()

	// Test with a more complex event structure
	fmt.Println("Stage 4: Testing with a complex event structure...")
	complexEvent := hec.Event{
		Event: map[string]interface{}{
			"message":   "HEC complex event test from spexma",
			"test":      "complex_structure_test",
			"timestamp": time.Now().Format(time.RFC3339),
			"numeric":   123.456,
			"boolean":   true,
			"nested_data": map[string]interface{}{
				"field1": "value1",
				"field2": 42,
				"field3": false,
			},
			"array_data": []string{"item1", "item2", "item3"},
		},
		SourceType: testSourcetype,
	}

	if err := hecClient.SendEvent(complexEvent); err != nil {
		fmt.Printf("ERROR: Complex event test failed: %v\n", err)
		fmt.Println("NOTE: This may indicate issues with complex data structures")
	} else {
		fmt.Println("SUCCESS: Complex event test passed")
	}
	fmt.Println()

	// Final summary
	fmt.Println("Test Summary")
	fmt.Println("===========")
	fmt.Println("All basic connectivity tests completed.")
	fmt.Println("If you encountered any errors, review the output above for details.")
	fmt.Println()
	fmt.Println("To verify events were received, check your Splunk instance with the search:")
	fmt.Printf("sourcetype=\"%s\"\n", testSourcetype)
}
