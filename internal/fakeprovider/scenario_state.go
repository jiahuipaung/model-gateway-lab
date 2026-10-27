package fakeprovider

import (
	"context"
	"sync"
)

type ScenarioState interface {
	Save(context.Context, string, string, bool) error
	Find(context.Context, string) (remoteID string, found bool, err error)
	AdvancePoll(context.Context, string) (pollCount int, shouldFail bool, found bool, err error)
}

type memoryScenarioState struct {
	mu      sync.Mutex
	byKey   map[string]string
	records map[string]memoryScenarioRecord
}

type memoryScenarioRecord struct {
	shouldFail bool
	pollCount  int
}

func NewMemoryScenarioState() ScenarioState {
	return &memoryScenarioState{
		byKey:   make(map[string]string),
		records: make(map[string]memoryScenarioRecord),
	}
}

func (s *memoryScenarioState) Save(_ context.Context, requestKey, remoteID string, shouldFail bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey[requestKey] = remoteID
	if _, exists := s.records[remoteID]; !exists {
		s.records[remoteID] = memoryScenarioRecord{shouldFail: shouldFail}
	}
	return nil
}

func (s *memoryScenarioState) Find(_ context.Context, requestKey string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	remoteID, found := s.byKey[requestKey]
	return remoteID, found, nil
}

func (s *memoryScenarioState) AdvancePoll(_ context.Context, remoteID string) (int, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[remoteID]
	if !found {
		return 0, false, false, nil
	}
	record.pollCount++
	s.records[remoteID] = record
	return record.pollCount, record.shouldFail, true, nil
}
