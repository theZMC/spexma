package publish

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/thezmc/spexma/internal/publish/hec"
)

// Config holds configuration for transforming CSV records to Splunk events
type TransformerConfig struct {
	SourceType       string            // Default sourcetype to use
	TimeField        string            // Field name containing the timestamp
	TimeFormat       string            // Format string for parsing the timestamp
	ExcludeFields    []string          // Fields to exclude from the event
	FieldMappings    map[string]string // Map CSV column names to Splunk field names
	ConstantFields   map[string]string // Fields to add to every event with constant values
	StrictMapping    bool              // If true, only include fields in FieldMappings
	PreserveNulls    bool              // If true, preserve null/empty values
	Host             string            // Host value for events
	Source           string            // Source value for events
	Index            string            // Index value for events
	DiscardInvalid   bool              // If true, discard records with invalid timestamps
	DefaultTimestamp *time.Time        // Default timestamp to use if not present or invalid
	TimeOffset       time.Duration     // Offset to apply to the timestamp
}

// NewDefaultConfig creates a default transformer configuration
func NewDefaultTransformerConfig() *TransformerConfig {
	return &TransformerConfig{
		TimeField:     "_time",    // Default Splunk time field
		TimeFormat:    "epoch",    // Default Splunk time format
		ExcludeFields: []string{}, // No excluded fields by default
		FieldMappings: map[string]string{},
		StrictMapping: false,
		PreserveNulls: false,
	}
}

// Transformer converts CSV records to Splunk events
type Transformer struct {
	config *TransformerConfig
}

// NewTransformer creates a new transformer with the given configuration
func NewTransformer(config *TransformerConfig) *Transformer {
	if config == nil {
		config = NewDefaultTransformerConfig()
	}
	return &Transformer{
		config: config,
	}
}

// Config returns the transformer's configuration
func (t *Transformer) Config() *TransformerConfig {
	return t.config
}

// TransformCSV reads a CSV file and returns Splunk events
func (t *Transformer) TransformCSV(reader io.Reader) ([]hec.Event, error) {
	csvReader := csv.NewReader(reader)

	// Read the header
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %w", err)
	}

	// Create a map to store header indices for quick lookups
	headerIndices := make(map[string]int)
	for i, h := range header {
		headerIndices[h] = i
	}

	// Check if the time field exists
	timeIndex := -1
	if t.config.TimeField != "" {
		var ok bool
		timeIndex, ok = headerIndices[t.config.TimeField]
		if !ok && !t.config.DiscardInvalid && t.config.DefaultTimestamp == nil {
			return nil, fmt.Errorf("time field '%s' not found in CSV header", t.config.TimeField)
		}
	}

	// Create a set of excluded fields for quick lookups
	excludeFields := make(map[string]bool)
	for _, field := range t.config.ExcludeFields {
		excludeFields[field] = true
	}

	// Process records
	var events []hec.Event
	lineNum := 1 // Start at 1 because we already read the header

	for {
		lineNum++
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV at line %d: %w", lineNum, err)
		}

		// Transform the record
		event, err := t.transformRecord(record, header, headerIndices, timeIndex, excludeFields)
		if err != nil {
			if t.config.DiscardInvalid {
				// Skip this record if it's invalid and we're configured to discard
				continue
			}
			return nil, fmt.Errorf("error transforming record at line %d: %w", lineNum, err)
		}

		// Apply time offset if specified
		if t.config.TimeOffset != 0 {
			applyTimeOffsets(&event, t.config.TimeOffset)
		}

		events = append(events, event)
	}

	return events, nil
}

var (
	// Regular expressions for matching timestamps as strings
	rfc3339FullRGX = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$`)

	// Regular expressions for matching timestamps in strings
	rfc3339RGX = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?`)
)

func applyTimeOffsets(event *hec.Event, offset time.Duration) {
	if event.Time != nil {
		// Apply the time offset
		*event.Time += int64(offset.Seconds())
	}

	// Recurse through nested fields checking for time data
	for key, value := range event.Event {
		switch v := value.(type) {
		case map[string]any:
			// If the field is a map, recurse into it
			nestedEvent := hec.Event{
				Event: v,
			}
			applyTimeOffsets(&nestedEvent, offset)
			event.Event[key] = nestedEvent.Event
		case []any:
			// If the field is a slice, iterate through it
			for i, item := range v {
				if nestedMap, ok := item.(map[string]any); ok {
					nestedEvent := hec.Event{
						Event: nestedMap,
					}
					applyTimeOffsets(&nestedEvent, offset)
					v[i] = nestedEvent.Event
				}
			}
		case string:
			// Check if the string **is** a timestamp
			switch {
			case rfc3339FullRGX.MatchString(v):
				// Parse the timestamp
				timestamp, err := dateparse.ParseAny(v)
				if err == nil {
					// Apply the time offset
					timestamp = timestamp.Add(offset)
					event.Event[key] = timestamp.Format(time.RFC3339)
				}
			case rfc3339RGX.MatchString(v):
				matchIndex := rfc3339RGX.FindAllStringIndex(v, -1)
				for _, match := range matchIndex {
					// Extract the timestamp
					timestampStr := v[match[0]:match[1]]
					timestamp, err := dateparse.ParseAny(v)

					if err == nil {
						// Apply the time offset
						timestamp = timestamp.Add(offset)
						event.Event[key] = strings.Replace(v, timestampStr, timestamp.Format(time.RFC3339), 1)
					}
				}
			}
		}
	}
}

// transformRecord converts a single CSV record to a Splunk event
func (t *Transformer) transformRecord(record []string, header []string, headerIndices map[string]int,
	timeIndex int, excludeFields map[string]bool,
) (hec.Event, error) {
	event := hec.Event{
		SourceType: t.config.SourceType,
		Host:       t.config.Host,
		Source:     t.config.Source,
		Index:      t.config.Index,
		Event:      make(map[string]any),
	}

	// Process timestamp if specified
	if timeIndex >= 0 && timeIndex < len(record) {
		timestamp, err := t.parseTimestamp(record[timeIndex])
		if err != nil {
			fmt.Printf("error parsing timestamp: %v", err)
			if t.config.DiscardInvalid {
				return event, fmt.Errorf("invalid timestamp: %w", err)
			}
			// Use default timestamp if available
			if t.config.DefaultTimestamp != nil {
				unixTime := t.config.DefaultTimestamp.Unix()
				event.Time = &unixTime
			}
		} else {
			event.Time = &timestamp
		}
	} else if t.config.DefaultTimestamp != nil {
		// Use default timestamp if no time field or out of range
		unixTime := t.config.DefaultTimestamp.Unix()
		event.Time = &unixTime
	}

	rawIsJSON := false

	// Check if _raw is a valid JSON object
	if rawIndex, ok := headerIndices["_raw"]; ok && rawIndex < len(record) {
		rawValue := record[rawIndex]
		if rawValue != "" {
			rawJSON := make(map[string]any)
			if err := json.Unmarshal([]byte(rawValue), &rawJSON); err == nil {
				// If _raw is a valid JSON object, make it the event
				event.Event = rawJSON
				rawIsJSON = true
			}
		}
	}

	// Process fields
	for i, value := range record {
		if i >= len(header) {
			// Skip if we have more values than headers
			continue
		}

		fieldName := header[i]

		switch fieldName {
		case t.config.TimeField:
			// Skip time field since we already processed it
			continue
		case "_raw":
			if rawIsJSON {
				continue
			}
			// If _raw is not JSON, treat it as a string field
			if value != "" {
				event.Event["_raw"] = value
			}
		case "host":
			if t.config.Host != "" {
				event.Host = t.config.Host
				continue
			}
		case "source":
			if t.config.Source != "" {
				event.Source = t.config.Source
				continue
			}
		case "sourcetype":
			if t.config.SourceType != "" {
				event.SourceType = t.config.SourceType
				continue
			}
		case "index":
			if t.config.Index != "" {
				event.Index = t.config.Index
				continue
			}
		case "punct":
			// Skip punctuation field
			continue
		}

		if rawIsJSON {
			// If _raw is JSON, skip all other fields
			continue
		}

		// Skip excluded fields
		if excludeFields[fieldName] {
			continue
		}

		// Skip time field since we already processed it
		if i == timeIndex {
			continue
		}

		// Apply field mappings if available
		if mappedName, ok := t.config.FieldMappings[fieldName]; ok {
			fieldName = mappedName
		} else if t.config.StrictMapping {
			// Skip fields not in the mapping if strict mode is on
			continue
		}

		// Skip empty values if not preserving nulls
		if value == "" && !t.config.PreserveNulls {
			continue
		}

		// Add the field to the event
		event.Event[fieldName] = value
	}

	// Add constant fields
	for key, value := range t.config.ConstantFields {
		event.Event[key] = value
	}

	return event, nil
}

// parseTimestamp converts a string timestamp to epoch seconds
func (t *Transformer) parseTimestamp(timeStr string) (int64, error) {
	// Handle empty timestamp
	tm, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return 0, fmt.Errorf("error parsing timestamp: %w", err)
	}

	// Convert to epoch seconds
	return tm.Unix(), nil
}
