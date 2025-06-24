package raft

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
	if lastLog.Term < candidateTerm {
		return false
	}
	if lastLog.Term == candidateTerm && lastLog.Index <= candidateIndex {
		return false
	}
	return true
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
