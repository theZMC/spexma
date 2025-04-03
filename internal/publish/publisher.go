package publish

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/thezmc/spexma/internal/publish/hec"
)

// Progress contains information about the publishing progress
type Progress struct {
	TotalFiles       int
	ProcessedFiles   int
	TotalEvents      int
	PublishedEvents  int
	FailedEvents     int
	CurrentFile      string
	mu               sync.RWMutex
	Status           string
	ElapsedTime      time.Duration
	StartTime        time.Time
	EstimatedEndTime time.Time
}

// UpdateProgress updates the progress information
func (p *Progress) UpdateProgress(processedFiles, totalEvents, publishedEvents, failedEvents int, currentFile string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ProcessedFiles = processedFiles
	p.TotalEvents = totalEvents
	p.PublishedEvents = publishedEvents
	p.FailedEvents = failedEvents
	p.CurrentFile = currentFile
	p.ElapsedTime = time.Since(p.StartTime)

	// Calculate estimated end time
	if p.TotalEvents > 0 && p.PublishedEvents > 0 {
		remainingEvents := p.TotalEvents - p.PublishedEvents
		eventRate := float64(p.PublishedEvents) / p.ElapsedTime.Seconds()
		if eventRate > 0 {
			remainingTime := time.Duration(float64(remainingEvents) / eventRate * float64(time.Second))
			p.EstimatedEndTime = time.Now().Add(remainingTime)
		}
	}
}

// SetStatus updates the status message
func (p *Progress) SetStatus(status string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Status = status
}

// GetStats returns the current progress stats
func (p *Progress) GetStats() (int, int, int, int, string, time.Duration, string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ProcessedFiles, p.TotalFiles, p.PublishedEvents, p.TotalEvents, p.CurrentFile, p.ElapsedTime, p.Status
}

// PublisherConfig contains configuration for the publisher
type PublisherConfig struct {
	HECClient       *hec.Client
	Transformer     *Transformer
	Concurrency     int
	BatchSize       int
	RetryCount      int
	RetryWait       time.Duration
	OutputDirectory string
	ProgressCh      chan<- *Progress
	DryRun          bool
	Debug           bool
}

// Publisher handles the publishing of events to Splunk HEC
type Publisher struct {
	config    *PublisherConfig
	progress  *Progress
	errorChan chan error
	wg        sync.WaitGroup
	stopCh    chan struct{}
}

// NewPublisher creates a new publisher
func NewPublisher(config *PublisherConfig) *Publisher {
	if config.RetryCount <= 0 {
		config.RetryCount = 3
	}
	if config.RetryWait <= 0 {
		config.RetryWait = 2 * time.Second
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 4
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	return &Publisher{
		config:    config,
		progress:  &Progress{StartTime: time.Now()},
		errorChan: make(chan error, 100),
		stopCh:    make(chan struct{}),
	}
}

// PublishDirectory processes all CSV files in a directory and publishes events to Splunk HEC
func (p *Publisher) PublishDirectory(directory string) error {
	// Get all CSV files in the directory
	files, err := filepath.Glob(filepath.Join(directory, "*.csv"))
	if err != nil {
		return fmt.Errorf("error finding CSV files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no CSV files found in directory: %s", directory)
	}

	if p.config.Debug {
		log.Printf("DEBUG: Found %d CSV files to process in %s", len(files), directory)
	}

	// Update progress
	p.progress.TotalFiles = len(files)
	p.progress.SetStatus("Starting")

	// Make a channel for file paths
	filesCh := make(chan string, len(files))

	// Start worker goroutines
	for i := 0; i < p.config.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(filesCh)
	}

	// Feed file paths to workers
	for _, file := range files {
		filesCh <- file
	}
	close(filesCh)

	// Wait for all workers to finish
	p.wg.Wait()

	// Check if there were any errors
	select {
	case err := <-p.errorChan:
		return err
	default:
		return nil
	}
}

// worker processes files from the file channel
func (p *Publisher) worker(filesCh <-chan string) {
	defer p.wg.Done()

	for file := range filesCh {
		// Extract sourcetype from filename (remove path and extension)
		base := filepath.Base(file)
		sourcetype := strings.TrimSuffix(base, filepath.Ext(base))

		if p.config.Debug {
			log.Printf("DEBUG: Worker processing file: %s (sourcetype: %s)", file, sourcetype)
		}

		// Update progress
		p.progress.SetStatus(fmt.Sprintf("Processing %s", base))

		// Process the file
		if err := p.processFile(file, sourcetype); err != nil {
			if p.config.Debug {
				log.Printf("DEBUG: Error processing file %s: %v", file, err)
			}
			select {
			case p.errorChan <- fmt.Errorf("error processing file %s: %w", file, err):
			default:
				// Channel full, continue
			}
		}
	}
}

// processFile reads a CSV file and publishes events to Splunk HEC
func (p *Publisher) processFile(filePath, sourcetype string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	if p.config.Debug {
		log.Printf("DEBUG: Opened file %s for processing", filePath)
	}

	// Update the transformer config to use this sourcetype
	tConfig := p.config.Transformer.Config()
	tConfig.SourceType = sourcetype

	if p.config.Debug {
		log.Printf("DEBUG: Using sourcetype: %s for events from %s", sourcetype, filePath)
	}

	// Transform the CSV to events
	events, err := p.config.Transformer.TransformCSV(file)
	if err != nil {
		return fmt.Errorf("error transforming CSV: %w", err)
	}

	if p.config.Debug {
		log.Printf("DEBUG: Transformed %d events from %s", len(events), filePath)
		if len(events) > 0 {
			// Log a sample event for debugging
			eventBytes, _ := json.Marshal(events[0])
			log.Printf("DEBUG: Sample event: %s", string(eventBytes))
		}
	}

	// Update progress
	p.progress.UpdateProgress(
		p.progress.ProcessedFiles,
		p.progress.TotalEvents+len(events),
		p.progress.PublishedEvents,
		p.progress.FailedEvents,
		filepath.Base(filePath),
	)

	// If dry run, don't actually send events
	if p.config.DryRun {
		if p.config.Debug {
			log.Printf("DEBUG: Dry run - not sending %d events from %s", len(events), filePath)
		}
		p.progress.UpdateProgress(
			p.progress.ProcessedFiles+1,
			p.progress.TotalEvents,
			p.progress.PublishedEvents+len(events),
			p.progress.FailedEvents,
			"",
		)
		p.progress.SetStatus("Dry run - events not sent")
		return nil
	}

	// Send events in batches
	for i := 0; i < len(events); i += p.config.BatchSize {
		end := i + p.config.BatchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]

		if p.config.Debug {
			log.Printf("DEBUG: Sending batch %d-%d of %d events from %s", i, end, len(events), filePath)
		}

		// Try to send the batch with retries
		var sendErr error
		for retry := 0; retry < p.config.RetryCount; retry++ {
			if retry > 0 {
				if p.config.Debug {
					log.Printf("DEBUG: Retry %d/%d for batch %d-%d from %s",
						retry+1, p.config.RetryCount, i, end, filePath)
				}
				p.progress.SetStatus(fmt.Sprintf("Retrying batch (%d/%d)", retry+1, p.config.RetryCount))
				time.Sleep(p.config.RetryWait)
			}

			if err := p.config.HECClient.SendEvents(batch); err != nil {
				sendErr = err
				if p.config.Debug {
					log.Printf("DEBUG: Batch %d-%d from %s failed: %v", i, end, filePath, err)
				}
				continue
			}

			if p.config.Debug {
				log.Printf("DEBUG: Successfully sent batch %d-%d from %s", i, end, filePath)
			}
			sendErr = nil
			break
		}

		if sendErr != nil {
			p.progress.UpdateProgress(
				p.progress.ProcessedFiles,
				p.progress.TotalEvents,
				p.progress.PublishedEvents,
				p.progress.FailedEvents+len(batch),
				filepath.Base(filePath),
			)
			return fmt.Errorf("failed to send events after %d retries: %w", p.config.RetryCount, sendErr)
		}

		// Update progress
		p.progress.UpdateProgress(
			p.progress.ProcessedFiles,
			p.progress.TotalEvents,
			p.progress.PublishedEvents+len(batch),
			p.progress.FailedEvents,
			filepath.Base(filePath),
		)

		// Send progress update if channel is available
		if p.config.ProgressCh != nil {
			select {
			case p.config.ProgressCh <- p.progress:
			default:
				// Channel full or nil, continue
			}
		}
	}

	// Mark file as processed
	p.progress.UpdateProgress(
		p.progress.ProcessedFiles+1,
		p.progress.TotalEvents,
		p.progress.PublishedEvents,
		p.progress.FailedEvents,
		"",
	)

	if p.config.Debug {
		log.Printf("DEBUG: Completed processing file %s with %d events", filePath, len(events))
	}

	return nil
}

// GetProgress returns the current progress
func (p *Publisher) GetProgress() *Progress {
	return p.progress
}
