package raft

import (
	"log/slog"

	raftpb "github.com/JunNishimura/graft/raft/grpc"
)

type Index uint64

type Term uint64

type LogData struct {
	Key   string
	Value uint64
}

type Log struct {
	Term  Term
	Index Index
	Data  *LogData
}

type Logs []*Log

func (l Logs) IsMoreUpToDate(candidateTerm Term, candidateIndex Index) bool {
	if len(l) == 0 {
		return false
	}

	lastLog := l[len(l)-1]
	if lastLog.Term > candidateTerm {
		return true
	}
	if lastLog.Term == candidateTerm && lastLog.Index > candidateIndex {
		return true
	}
	return false
}

func (l Logs) LastIndex() Index {
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1].Index
}

func (l Logs) LastTerm() Term {
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1].Term
}

func (l Logs) containsLog(term Term, index Index) bool {
	if len(l) == 0 {
		return term == 0 && index == 0
	}

	if l.LastIndex() < index {
		return false
	}

	for i := len(l) - 1; i >= 0; i-- {
		if l[i].Index == index {
			return l[i].Term == term
		}
	}

	return false
}

func (l Logs) FindByIndex(index Index) *Log {
	if l.LastIndex() < index {
		return nil
	}

	for i := len(l) - 1; i >= 0; i-- {
		if l[i].Index == index {
			return l[i]
		}
	}

	return nil
}

func (l Logs) DeleteAllAfter(index Index) Logs {
	if l.LastIndex() < index {
		return l
	}

	for i := len(l) - 1; i >= 0; i-- {
		if l[i].Index == index {
			return l[:i]
		}
	}

	return l
}

func (l Logs) Append(logs ...*Log) Logs {
	if len(logs) == 0 {
		return l
	}

	if len(l) == 0 {
		return logs
	}

	lastIndex := l.LastIndex()
	for i, log := range logs {
		if log.Index != lastIndex+Index(i+1) {
			slog.Warn("logs are not sequential", "expected_index", lastIndex+Index(i+1), "actual_index", log.Index)
		}

		l = append(l, log)
	}

	return l
}

func convertToLogs(data map[string]interface{}, term Term, lastIndex Index) Logs {
	logs := make(Logs, 0, len(data))
	count := Index(0)
	for key, value := range data {
		count++

		intValue, ok := value.(uint64)
		if !ok {
			// If the value is not a uint64, we skip it.
			slog.Warn("skipping non-uint64 value in log conversion", "key", key, "value", value)
			continue
		}

		logs = append(logs, &Log{
			Term:  term,
			Index: lastIndex + count,
			Data: &LogData{
				Key:   key,
				Value: intValue,
			},
		})
	}
	return logs
}

func (l Logs) convertToLogEntries() []*raftpb.Entry {
	entries := make([]*raftpb.Entry, 0, len(l))
	for _, log := range l {
		entries = append(entries, &raftpb.Entry{
			Term:  uint64(log.Term),
			Index: uint64(log.Index),
			Data: &raftpb.EntryData{
				Key:   log.Data.Key,
				Value: log.Data.Value,
			},
		})
	}
	return entries
}
