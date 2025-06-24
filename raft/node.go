package raft

import (
	"context"
	"log"
	"math"
	"time"

	raftpb "github.com/JunNishimura/graft/raft/grpc"
	"github.com/JunNishimura/graft/rand"
	"github.com/JunNishimura/graft/tokens"
)

type ID string

func NewID() ID {
	ulid := tokens.GenerateULID()
	return ID(ulid)
}

type StateMachine struct {
	m map[string]uint64
}

type Node struct {
	id           ID
	leaderID     *ID
	timeoutTimer *time.Timer
	state        State
	currentTerm  Term
	votedFor     *ID
	logs         Logs
	commitIndex  Index
	lastApplied  Index
	nextIndex    []Index
	matchIndex   []Index

	stateMachine *StateMachine

	clients []raftpb.RaftServiceClient
	raftpb.UnimplementedRaftServiceServer
}

const (
	electionTimeoutLowerBound = 150 * time.Millisecond
	electionTimeoutUpperBound = 300 * time.Millisecond
	heartbeatInterval         = 100 * time.Millisecond
)

var (
	_ raftpb.RaftServiceServer = (*Node)(nil)
)

func NewNode(clients []raftpb.RaftServiceClient) *Node {
	return &Node{
		id: NewID(),
		timeoutTimer: time.NewTimer(rand.GenerateDuration(
			electionTimeoutLowerBound,
			electionTimeoutUpperBound,
		)),
		state:       StateFollower,
		currentTerm: 0,
		votedFor:    nil,
		logs:        make(Logs, 0),
		commitIndex: 0,
		lastApplied: 0,
		clients:     clients,
		stateMachine: &StateMachine{
			m: make(map[string]uint64),
		},
	}
}

func (n *Node) Run(ctx context.Context) {
	for {
		select {
		case <-n.timeoutTimer.C:
			n.startElection(ctx)
		}
	}
}

func (n *Node) startElection(ctx context.Context) {
	// Election timeout reached, start a new election
	n.state = StateCandidate
	n.currentTerm++
	n.votedFor = &n.id
	n.timeoutTimer.Reset(rand.GenerateDuration(
		electionTimeoutLowerBound,
		electionTimeoutUpperBound,
	))

	respCh := make(chan *raftpb.RequestVoteResponse, len(n.clients))
	for _, client := range n.clients {
		go func(client raftpb.RaftServiceClient) {
			req := &raftpb.RequestVoteRequest{
				Term:         uint64(n.currentTerm),
				CandidateId:  string(n.id),
				LastLogIndex: uint64(n.logs.LastIndex()),
				LastLogTerm:  uint64(n.logs.LastTerm()),
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				log.Printf("failed to send RequestVoteRequest: %v", err)
				return
			}

			respCh <- resp
		}(client)
	}

	voteCount := 1 // Count the vote for itself
	for resp := range respCh {
		if resp.Term > uint64(n.currentTerm) {
			n.state = StateFollower
			n.votedFor = nil
			n.currentTerm = Term(resp.Term)
			n.timeoutTimer.Reset(rand.GenerateDuration(
				electionTimeoutLowerBound,
				electionTimeoutUpperBound,
			))
			close(respCh)
			return
		}

		if !resp.VoteGranted {
			continue
		}

		voteCount++
		if n.isElectionWinner(voteCount) {
			close(respCh)
			n.becomeLeader(ctx)
		}
	}
}

func (n *Node) isElectionWinner(voteCount int) bool {
	// If the node has received more than half of the votes, it is the leader
	return voteCount > len(n.clients)/2
}

func (n *Node) becomeLeader(ctx context.Context) {
	n.state = StateLeader
	n.votedFor = nil

	// Initialize nextIndex as the last log index + 1 for each client
	n.nextIndex = make([]Index, len(n.clients))
	lastLogIndex := n.logs.LastIndex()
	for i := range n.nextIndex {
		n.nextIndex[i] = lastLogIndex + 1
	}
	n.matchIndex = make([]Index, len(n.clients))

	// Send heartbeat to all clients to establish leadership
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.heartbeat(ctx)
	}
}

func (n *Node) heartbeat(ctx context.Context) {
	// TODO: process concurrently
	for i, client := range n.clients {
		prevLogIndex := uint64(0)
		prevLogTerm := uint64(0)
		prevLog := n.logs.FindByIndex(n.nextIndex[i] - 1)
		if prevLog != nil {
			prevLogIndex = uint64(prevLog.Index)
			prevLogTerm = uint64(prevLog.Term)
		}

		req := &raftpb.AppendEntriesRequest{
			Term:         uint64(n.currentTerm),
			LeaderId:     string(n.id),
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil, // No new entries in heartbeat
			LeaderCommit: uint64(n.commitIndex),
		}

		resp, err := client.AppendEntries(ctx, req)
		if err != nil {
			log.Printf("failed to send AppendEntriesRequest: %v", err)
			continue
		}

		if resp.Term > uint64(n.currentTerm) {
			n.state = StateFollower
			n.votedFor = nil
			n.currentTerm = Term(resp.Term)
			n.timeoutTimer.Reset(rand.GenerateDuration(
				electionTimeoutLowerBound,
				electionTimeoutUpperBound,
			))
			return
		}

		if resp.Success {
			n.matchIndex[i] = n.nextIndex[i] - 1
		} else if n.nextIndex[i] > 0 {
			n.nextIndex[i]--
		}
	}
}

func (n *Node) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	// check if the request term is greater than or equal to the current term.
	if Term(req.Term) < n.currentTerm {
		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
	}

	if Term(req.Term) > n.currentTerm {
		n.timeoutTimer.Reset(rand.GenerateDuration(
			electionTimeoutLowerBound,
			electionTimeoutUpperBound,
		))
		n.currentTerm = Term(req.Term)
		n.state = StateFollower
		n.votedFor = nil // Reset votedFor when a new term is started
	}

	// check if the node has not voted yet or if it has voted for the candidate.
	if n.votedFor != nil && *n.votedFor != ID(req.CandidateId) {
		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
	}

	// check if the candidate's last log is at least as up-to-date as the node's last log
	if !n.logs.IsMoreUpToDate(Term(req.LastLogTerm), Index(req.LastLogIndex)) {
		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
	}

	// Grant the vote to the candidate
	candidateId := ID(req.CandidateId)
	n.votedFor = &candidateId
	return &raftpb.RequestVoteResponse{
		Term:        uint64(n.currentTerm),
		VoteGranted: true,
	}, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	// Reject the request if the term is less than the current term
	if Term(req.Term) < n.currentTerm {
		return &raftpb.AppendEntriesResponse{
			Term:    uint64(n.currentTerm),
			Success: false,
		}, nil
	}
	if Term(req.Term) >= n.currentTerm {
		n.timeoutTimer.Reset(rand.GenerateDuration(
			electionTimeoutLowerBound,
			electionTimeoutUpperBound,
		))
	}
	if Term(req.Term) > n.currentTerm {
		n.currentTerm = Term(req.Term)
		n.state = StateFollower
		n.votedFor = nil // Reset votedFor when a new term is started
		leaderID := ID(req.LeaderId)
		n.leaderID = &leaderID
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !n.logs.containsLog(Term(req.PrevLogTerm), Index(req.PrevLogIndex)) {
		return &raftpb.AppendEntriesResponse{
			Term:    uint64(n.currentTerm),
			Success: false,
		}, nil
	}

	// Delete conflicting log entries
	for _, entry := range req.Entries {
		matchLog := n.logs.FindByIndex(Index(entry.Index))
		if matchLog == nil || matchLog.Term == Term(entry.Term) {
			continue
		}

		// If the log entry conflicts a new one(same index but different term), delete the existing log entry and all entries after it
		n.logs = n.logs.DeleteAllAfter(Index(entry.Index))
	}

	// Append new log entries
	for _, entry := range req.Entries {
		logEntry := &Log{
			Index: Index(entry.Index),
			Term:  Term(entry.Term),
			Data: &LogData{
				Key:   entry.Data.Key,
				Value: entry.Data.Value,
			},
		}

		n.logs = append(n.logs, logEntry)
	}

	// Update commit index
	if req.LeaderCommit > uint64(n.commitIndex) {
		n.commitIndex = Index(math.Min(float64(req.LeaderCommit), float64(n.logs.LastIndex())))
	}

	return &raftpb.AppendEntriesResponse{
		Term:    uint64(n.currentTerm),
		Success: true,
	}, nil
}
