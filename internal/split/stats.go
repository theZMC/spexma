package split

import (
	"sync"
)

// Stats keeps track of the record counts per sourcetype
type Stats struct {
	mu               sync.RWMutex
	records          map[string]int
	order            []string // To maintain order of sourcetypes for display
	maxSourcetypeLen int      // Track maximum sourcetype length for display
	analyzedRecords  int      // Track number of records analyzed in first pass
	processingPhase  string   // Current processing phase
}

// NewStats creates a new Stats instance
func NewStats() *Stats {
	return &Stats{
		records:          make(map[string]int),
		order:            []string{},
		maxSourcetypeLen: 20, // Default starting width
		processingPhase:  "initializing",
	}
}

// IncrementRecord adds a record to the stats
func (s *Stats) IncrementRecord(sourcetype string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.records[sourcetype]; !exists {
		s.order = append(s.order, sourcetype)
		s.records[sourcetype] = 0

		// Update max sourcetype length if needed
		if len(sourcetype) > s.maxSourcetypeLen {
			s.maxSourcetypeLen = len(sourcetype)
		}
	}
	s.records[sourcetype]++
}

// IncrementAnalyzedRecords increments the count of analyzed records
func (s *Stats) IncrementAnalyzedRecords() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.analyzedRecords++
}

// SetProcessingPhase sets the current processing phase
func (s *Stats) SetProcessingPhase(phase string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processingPhase = phase
}

// GetProcessingPhase gets the current processing phase
func (s *Stats) GetProcessingPhase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.processingPhase
}

// GetAnalyzedRecords gets the number of analyzed records
func (s *Stats) GetAnalyzedRecords() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.analyzedRecords
}

// GetMaxSourcetypeLen gets the maximum sourcetype length
func (s *Stats) GetMaxSourcetypeLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxSourcetypeLen
}

// GetStats returns a copy of the stats for display
func (s *Stats) GetStats() ([]string, map[string]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	recordsCopy := make(map[string]int)
	for k, v := range s.records {
		recordsCopy[k] = v
	}

	orderCopy := make([]string, len(s.order))
	copy(orderCopy, s.order)

	return orderCopy, recordsCopy
}
