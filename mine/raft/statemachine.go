package raft

import "sync"

type StateMachine struct {
	mu sync.RWMutex
	m  map[string]uint64
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		m: make(map[string]uint64),
	}
}

func (s *StateMachine) Get(key string) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.m[key]
	return value, exists
}

func (s *StateMachine) Set(key string, value uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}
