package raft

type Index uint64

type Term uint64

func (t Term) IsVotable(candidateTerm Term) bool {
	return t <= candidateTerm
}

type Log struct {
	Term  Term
	Index Index
	Data  []byte
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
