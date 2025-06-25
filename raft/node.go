package raft

import (
	"context"
	"log/slog"
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

func (id ID) Equal(other ID) bool {
	return id == other
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

	cluster Cluster
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

func NewNode(nodeID ID, cluster Cluster) *Node {
	return &Node{
		id: nodeID,
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
		stateMachine: &StateMachine{
			m: make(map[string]uint64),
		},
		cluster: cluster,
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
	slog.InfoContext(ctx,
		"Election timeout reached, starting a new election",
		"node_id", n.id,
		"current_term", n.currentTerm)

	// Election timeout reached, start a new election
	n.state = StateCandidate
	n.currentTerm++
	n.votedFor = &n.id
	n.timeoutTimer.Reset(rand.GenerateDuration(
		electionTimeoutLowerBound,
		electionTimeoutUpperBound,
	))

	respCh := make(chan *raftpb.RequestVoteResponse, n.cluster.NodeCount()-1)
	for _, node := range n.cluster.OtherNodes(n.id) {
		client := node.RPCClient
		go func(client raftpb.RaftServiceClient) {
			req := &raftpb.RequestVoteRequest{
				Term:         uint64(n.currentTerm),
				CandidateId:  string(n.id),
				LastLogIndex: uint64(n.logs.LastIndex()),
				LastLogTerm:  uint64(n.logs.LastTerm()),
			}

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				slog.Error("failed to send RequestVoteRequest", "error", err)
				return
			}

			respCh <- resp
		}(client)
	}

	voteCount := 1 // Count the vote for itself
	for resp := range respCh {
		if resp.Term > uint64(n.currentTerm) {
			slog.InfoContext(ctx,
				"Received higher term during election, stepping down",
				"node_id", n.id,
				"received_term", resp.Term,
				"current_term", n.currentTerm)

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
			slog.InfoContext(ctx,
				"Election won, becoming leader",
				"node_id", n.id,
				"current_term", n.currentTerm,
				"vote_count", voteCount)

			close(respCh)
			n.becomeLeader(ctx)
		}
	}
}

func (n *Node) isElectionWinner(voteCount int) bool {
	// If the node has received more than half of the votes, it is the leader
	return voteCount > n.cluster.NodeCount()/2
}

func (n *Node) becomeLeader(ctx context.Context) {
	n.state = StateLeader
	n.votedFor = nil

	// Initialize nextIndex as the last log index + 1 for each client
	n.nextIndex = make([]Index, n.cluster.NodeCount())
	lastLogIndex := n.logs.LastIndex()
	for i := range n.nextIndex {
		n.nextIndex[i] = lastLogIndex + 1
	}
	n.matchIndex = make([]Index, n.cluster.NodeCount())

	// Send heartbeat to all clients to establish leadership
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.heartbeat(ctx)
	}
}

func (n *Node) heartbeat(ctx context.Context) {
	slog.InfoContext(ctx,
		"Sending heartbeat to all nodes",
		"node_id", n.id,
		"current_term", n.currentTerm)

	// TODO: process concurrently
	for i, node := range n.cluster.OtherNodes(n.id) {
		client := node.RPCClient
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
			slog.Error("failed to send AppendEntriesRequest", "error", err)
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
	slog.InfoContext(ctx,
		"Received RequestVoteRequest",
		"node_id", n.id,
		"request_term", req.Term,
		"candidate_id", req.CandidateId,
		"last_log_index", req.LastLogIndex,
		"last_log_term", req.LastLogTerm)

	// check if the request term is greater than or equal to the current term.
	if Term(req.Term) < n.currentTerm {
		slog.InfoContext(ctx,
			"Rejecting RequestVoteRequest due to lower term",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term)

		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
	}

	if Term(req.Term) > n.currentTerm {
		slog.InfoContext(ctx,
			"Stepping down to follower due to higher term in RequestVoteRequest",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term)

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
		slog.InfoContext(ctx,
			"Rejecting RequestVoteRequest due to already voted for another candidate",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"voted_for", n.votedFor,
			"candidate_id", req.CandidateId)

		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
	}

	// check if the candidate's last log is at least as up-to-date as the node's last log
	if !n.logs.IsMoreUpToDate(Term(req.LastLogTerm), Index(req.LastLogIndex)) {
		slog.InfoContext(ctx,
			"Rejecting RequestVoteRequest due to candidate's log not being up-to-date",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"candidate_id", req.CandidateId,
			"candidate_last_log_index", req.LastLogIndex,
			"candidate_last_log_term", req.LastLogTerm,
			"node_last_log_index", n.logs.LastIndex(),
			"node_last_log_term", n.logs.LastTerm())

		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
	}

	slog.InfoContext(ctx,
		"Granting vote to candidate",
		"node_id", n.id,
		"current_term", n.currentTerm,
		"candidate_id", req.CandidateId,
		"candidate_last_log_index", req.LastLogIndex,
		"candidate_last_log_term", req.LastLogTerm)

	// Grant the vote to the candidate
	candidateId := ID(req.CandidateId)
	n.votedFor = &candidateId
	return &raftpb.RequestVoteResponse{
		Term:        uint64(n.currentTerm),
		VoteGranted: true,
	}, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	slog.InfoContext(ctx,
		"Received AppendEntriesRequest",
		"node_id", n.id,
		"request_term", req.Term,
		"leader_id", req.LeaderId,
		"prev_log_index", req.PrevLogIndex,
		"prev_log_term", req.PrevLogTerm,
		"entries_count", len(req.Entries),
		"leader_commit", req.LeaderCommit)

	// Reject the request if the term is less than the current term
	if Term(req.Term) < n.currentTerm {
		slog.InfoContext(ctx,
			"Rejecting AppendEntriesRequest due to lower term",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term)

		return &raftpb.AppendEntriesResponse{
			Term:    uint64(n.currentTerm),
			Success: false,
		}, nil
	}
	if Term(req.Term) >= n.currentTerm {
		slog.InfoContext(ctx,
			"Resetting timeout timer due to AppendEntriesRequest",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term)

		n.timeoutTimer.Reset(rand.GenerateDuration(
			electionTimeoutLowerBound,
			electionTimeoutUpperBound,
		))
	}
	if Term(req.Term) > n.currentTerm {
		slog.InfoContext(ctx,
			"Stepping down to follower due to higher term in AppendEntriesRequest",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term)

		n.currentTerm = Term(req.Term)
		n.state = StateFollower
		n.votedFor = nil // Reset votedFor when a new term is started
		leaderID := ID(req.LeaderId)
		n.leaderID = &leaderID
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !n.logs.containsLog(Term(req.PrevLogTerm), Index(req.PrevLogIndex)) {
		slog.InfoContext(ctx,
			"Rejecting AppendEntriesRequest due to log mismatch",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term,
			"prev_log_index", req.PrevLogIndex,
			"prev_log_term", req.PrevLogTerm)

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
		slog.InfoContext(ctx,
			"Deleting conflicting log entries",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term,
			"conflicting_index", entry.Index,
			"conflicting_term", entry.Term)
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

		slog.InfoContext(ctx,
			"Appending new log entry",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term,
			"entry_index", entry.Index,
			"entry_term", entry.Term,
			"entry_key", entry.Data.Key,
			"entry_value", entry.Data.Value)
		n.logs = append(n.logs, logEntry)
	}

	// Update commit index
	if req.LeaderCommit > uint64(n.commitIndex) {
		newCommitIndex := Index(math.Min(float64(req.LeaderCommit), float64(n.logs.LastIndex())))
		slog.InfoContext(ctx,
			"Updating commit index",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term,
			"leader_commit", req.LeaderCommit,
			"new_commit_index", newCommitIndex)

		n.commitIndex = newCommitIndex
	}

	slog.InfoContext(ctx,
		"AppendEntriesRequest processed successfully",
		"node_id", n.id,
		"current_term", n.currentTerm,
		"request_term", req.Term,
		"prev_log_index", req.PrevLogIndex,
		"prev_log_term", req.PrevLogTerm,
		"entries_count", len(req.Entries),
		"leader_commit", req.LeaderCommit)
	return &raftpb.AppendEntriesResponse{
		Term:    uint64(n.currentTerm),
		Success: true,
	}, nil
}
