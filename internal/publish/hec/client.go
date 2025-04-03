package hec

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Event represents a single event to be sent to Splunk HEC
type Event struct {
	Time       *int64         `json:"time,omitempty"`
	Host       string         `json:"host,omitempty"`
	Source     string         `json:"source,omitempty"`
	SourceType string         `json:"sourcetype,omitempty"`
	Index      string         `json:"index,omitempty"`
	Event      map[string]any `json:"event"`
}

// Client represents a Splunk HEC client
type Client struct {
	URL             string
	Token           string
	InsecureSSL     bool
	Timeout         time.Duration
	BatchSize       int
	HTTPClient      *http.Client
	DefaultIndex    string
	DefaultHost     string
	DefaultSource   string
	DefaultMetadata map[string]string
	Debug           bool
}

// Options for configuring the HEC client
type Options struct {
	InsecureSSL     bool
	Timeout         time.Duration
	BatchSize       int
	DefaultIndex    string
	DefaultHost     string
	DefaultSource   string
	DefaultMetadata map[string]string
	Debug           bool
}

// Response from the Splunk HEC API
type Response struct {
	Text    string `json:"text"`
	Code    int    `json:"code"`
	Invalid int    `json:"invalid,omitempty"`
}

// NewClient creates a new Splunk HEC client
func NewClient(url, token string, options *Options) *Client {
	if options == nil {
		options = &Options{
			Timeout:   30 * time.Second,
			BatchSize: 100,
			Debug:     false,
		}
	}

	// Set up HTTP client with proper timeout and TLS configuration
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: options.InsecureSSL},
	}
	httpClient := &http.Client{
		Timeout:   options.Timeout,
		Transport: transport,
	}

	return &Client{
		URL:             url,
		Token:           token,
		InsecureSSL:     options.InsecureSSL,
		Timeout:         options.Timeout,
		BatchSize:       options.BatchSize,
		HTTPClient:      httpClient,
		DefaultIndex:    options.DefaultIndex,
		DefaultHost:     options.DefaultHost,
		DefaultSource:   options.DefaultSource,
		DefaultMetadata: options.DefaultMetadata,
		Debug:           options.Debug,
	}
}

// SendEvent sends a single event to Splunk HEC
func (c *Client) SendEvent(event Event) error {
	// Apply defaults if not set
	if event.Index == "" {
		event.Index = c.DefaultIndex
	}
	if event.Host == "" {
		event.Host = c.DefaultHost
	}
	if event.Source == "" {
		event.Source = c.DefaultSource
	}

	if c.Debug {
		log.Printf("DEBUG: Sending single event with sourcetype: %s", event.SourceType)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	return c.sendPayload(payload)
}

// SendEvents sends multiple events to Splunk HEC
func (c *Client) SendEvents(events []Event) error {
	if len(events) == 0 {
		return nil
	}

	if c.Debug {
		log.Printf("DEBUG: Preparing to send %d events", len(events))
	}

	// Apply defaults to each event
	for i := range events {
		if events[i].Index == "" {
			events[i].Index = c.DefaultIndex
		}
		if events[i].Host == "" {
			events[i].Host = c.DefaultHost
		}
		if events[i].Source == "" {
			events[i].Source = c.DefaultSource
		}
	}

	// For smaller batches, send directly
	if len(events) <= c.BatchSize {
		payload, err := json.Marshal(events)
		if err != nil {
			return fmt.Errorf("error marshaling events: %w", err)
		}
		return c.sendPayload(payload)
	}

	// For larger batches, split into smaller chunks
	if c.Debug {
		log.Printf("DEBUG: Splitting %d events into batches of %d", len(events), c.BatchSize)
	}

	for i := 0; i < len(events); i += c.BatchSize {
		end := i + c.BatchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[i:end]

		if c.Debug {
			log.Printf("DEBUG: Sending batch %d-%d of %d", i, end, len(events))
		}

		payload, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("error marshaling events batch %d-%d: %w", i, end, err)
		}

		if err := c.sendPayload(payload); err != nil {
			return fmt.Errorf("error sending events batch %d-%d: %w", i, end, err)
		}
	}

	if c.Debug {
		log.Printf("DEBUG: Successfully sent all %d events", len(events))
	}

	return nil
}

// sendPayload sends a JSON payload to Splunk HEC
func (c *Client) sendPayload(payload []byte) error {
	if c.Debug {
		log.Printf("DEBUG: Sending payload to %s (length: %d bytes)", c.URL, len(payload))
		// Print the first part of the payload for debugging (limit to avoid flooding logs)
		if len(payload) < 1000 {
			log.Printf("DEBUG: Payload: %s", string(payload))
		} else {
			log.Printf("DEBUG: Payload (truncated): %s...", string(payload[:1000]))
		}
	}

	req, err := http.NewRequest("POST", c.URL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Splunk "+c.Token)

	// Add any custom headers from DefaultMetadata
	for key, value := range c.DefaultMetadata {
		req.Header.Set(key, value)
	}

	if c.Debug {
		log.Printf("DEBUG: Request headers: %v", req.Header)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	if c.Debug {
		log.Printf("DEBUG: Response status: %s", resp.Status)
		log.Printf("DEBUG: Response body: %s", string(body))
	}

	var hecResponse Response
	if err := json.Unmarshal(body, &hecResponse); err != nil {
		if c.Debug {
			log.Printf("DEBUG: Failed to parse response as JSON: %v", err)
		}
		return fmt.Errorf("error parsing response: %w", err)
	}

	if hecResponse.Code != 0 {
		return fmt.Errorf("HEC error: %s (code: %d)", hecResponse.Text, hecResponse.Code)
	}

	if c.Debug {
		log.Printf("DEBUG: Successfully sent payload")
	}

	return nil
}

// HealthCheck validates connectivity to the HEC endpoint
func (c *Client) HealthCheck() error {
	// Send a minimal event to check connectivity
	testEvent := Event{
		Event: map[string]interface{}{
			"message": "HEC connection test from spexma",
		},
		SourceType: "spexma:test",
	}

	if c.Debug {
		log.Println("DEBUG: Performing HEC health check")
	}

	if err := c.SendEvent(testEvent); err != nil {
		return fmt.Errorf("HEC health check failed: %w", err)
	}

	if c.Debug {
		log.Println("DEBUG: HEC health check passed successfully")
	}

	return nil
}
