package raft

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"sync"
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

func (s *StateMachine) Get(key string) (uint64, bool) {
	value, exists := s.m[key]
	return value, exists
}

func (s *StateMachine) Set(key string, value uint64) {
	s.m[key] = value
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

	mu sync.RWMutex

	cluster Cluster
	raftpb.UnimplementedRaftServiceServer
}

const (
	electionTimeoutLowerBound = 150 * time.Millisecond
	electionTimeoutUpperBound = 300 * time.Millisecond
	heartbeatInterval         = 50 * time.Millisecond
)

var (
	_ raftpb.RaftServiceServer = (*Node)(nil)
)

func NewNode(ctx context.Context, nodeID ID, cluster Cluster) *Node {
	node := &Node{
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

	slog.InfoContext(ctx, "Node initialized", "node_id", nodeID)
	go node.waitForElectionTimeout(ctx)

	return node
}

func (n *Node) waitForElectionTimeout(ctx context.Context) {
	<-n.timeoutTimer.C

	n.mu.RLock()
	isLeader := n.state == StateLeader
	n.mu.RUnlock()

	if !isLeader {
		n.startElection(ctx)
	}
}

func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
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
	n.mu.Unlock()

	voteCount := 1 // Count the vote for itself
	for _, node := range n.cluster.OtherNodes(n.id) {
		go func(node *NodeInfo) {
			client := node.RPCClient
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

			n.mu.Lock()
			if resp.Term > uint64(n.currentTerm) {
				slog.InfoContext(ctx,
					"Received higher term during election, stepping down",
					"node_id", n.id,
					"received_term", resp.Term,
					"current_term", n.currentTerm)

				n.becomeFollower(ctx, Term(resp.Term))
				n.mu.Unlock()
				return
			}

			if !resp.VoteGranted {
				n.mu.Unlock()
				return
			}

			voteCount++
			if n.isOverMajority(voteCount) {
				n.mu.Unlock()
				n.becomeLeader(ctx)
				return
			}
		}(node)
	}

	// If the node has not won the election, reset the timer and wait for the next election timeout
	go n.waitForElectionTimeout(ctx)
}

func (n *Node) isOverMajority(voteCount int) bool {
	return voteCount > n.cluster.NodeCount()/2
}

// expect lock to be held
func (n *Node) becomeFollower(ctx context.Context, term Term) {
	n.state = StateFollower
	n.votedFor = nil
	n.currentTerm = Term(term)
	n.timeoutTimer.Reset(rand.GenerateDuration(electionTimeoutLowerBound, electionTimeoutUpperBound))
	go n.waitForElectionTimeout(ctx)
}

func (n *Node) becomeLeader(ctx context.Context) {
	n.mu.Lock()
	slog.InfoContext(ctx,
		"Election won, becoming leader",
		"node_id", n.id)

	n.state = StateLeader
	n.votedFor = nil

	// Initialize nextIndex as the last log index + 1 for each client
	n.nextIndex = make([]Index, n.cluster.NodeCount())
	lastLogIndex := n.logs.LastIndex()
	for i := range n.nextIndex {
		n.nextIndex[i] = lastLogIndex + 1
	}
	n.matchIndex = make([]Index, n.cluster.NodeCount())

	n.mu.Unlock()

	go func() {
		// Send heartbeat to all clients to establish leadership
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			n.heartbeat(ctx)

			<-ticker.C

			n.mu.RLock()
			if n.state != StateLeader {
				slog.InfoContext(ctx,
					"Stopping heartbeat as node is no longer leader",
					"node_id", n.id,
					"current_term", n.currentTerm)
				n.mu.RUnlock()
				return
			}
			n.mu.RUnlock()
		}
	}()
}

func (n *Node) heartbeat(ctx context.Context) {
	slog.InfoContext(ctx,
		"Sending heartbeat to all nodes",
		"node_id", n.id)

	for i, node := range n.cluster.OtherNodes(n.id) {
		go func(i int, node *NodeInfo) {
			client := node.RPCClient
			prevLogIndex := uint64(0)
			prevLogTerm := uint64(0)

			n.mu.Lock()
			defer n.mu.Unlock()
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
				return
			}

			if resp.Term > uint64(n.currentTerm) {
				slog.InfoContext(ctx,
					"Received higher term during heartbeat, stepping down",
					"node_id", n.id,
					"received_term", resp.Term,
					"current_term", n.currentTerm)

				n.becomeFollower(ctx, Term(resp.Term))
				return
			}

			if resp.Success {
				slog.InfoContext(ctx,
					"Heartbeat successful",
					"node_id", n.id,
					"current_term", n.currentTerm,
					"next_index", n.nextIndex[i],
					"match_index", n.matchIndex[i])

				n.matchIndex[i] = n.nextIndex[i] - 1
			} else if n.nextIndex[i] > 0 {
				slog.InfoContext(ctx,
					"Heartbeat failed, decrementing nextIndex",
					"node_id", n.id,
					"current_term", n.currentTerm,
					"next_index", n.nextIndex[i])

				n.nextIndex[i]--
			}
		}(i, node)
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
	n.mu.Lock()
	defer n.mu.Unlock()
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

	if Term(req.Term) > n.currentTerm && n.state != StateFollower {
		slog.InfoContext(ctx,
			"Stepping down to follower due to higher term in RequestVoteRequest",
			"node_id", n.id,
			"current_term", n.currentTerm,
			"request_term", req.Term)

		n.becomeFollower(ctx, Term(req.Term))
		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
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
	if n.logs.IsMoreUpToDate(Term(req.LastLogTerm), Index(req.LastLogIndex)) {
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
	n.state = StateFollower
	n.timeoutTimer.Reset(rand.GenerateDuration(
		electionTimeoutLowerBound,
		electionTimeoutUpperBound,
	))
	n.currentTerm = Term(req.Term)
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

	n.mu.Lock()
	defer n.mu.Unlock()
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

	// search minimum index in the entries
	if len(req.Entries) != 0 {
		minEntry := req.Entries[0]
		for _, entry := range req.Entries {
			if entry.Index < minEntry.Index {
				minEntry = entry
			}
		}

		// Delete conflicting log entries
		matchLog := n.logs.FindByIndex(Index(minEntry.Index))
		if matchLog != nil && matchLog.Term != Term(minEntry.Term) {
			// If the log entry conflicts a new one(same index but different term), delete the existing log entry and all entries after it
			slog.InfoContext(ctx,
				"Deleting conflicting log entries",
				"node_id", n.id,
				"current_term", n.currentTerm,
				"request_term", req.Term,
				"conflicting_index", minEntry.Index,
				"conflicting_term", minEntry.Term)
			n.logs = n.logs.DeleteAllAfter(Index(minEntry.Index))
		}
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

func (n *Node) HandleClientRequest(ctx context.Context) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/raft/append" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		// decode the request body
		var reqData map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		// Process the client request here
		slog.InfoContext(ctx, "Processing client request", "request", reqData)

		n.mu.Lock()

		// Convert the request to a log entry
		newLogs := convertToLogs(reqData, n.currentTerm, n.logs.LastIndex())
		if len(newLogs) == 0 {
			http.Error(w, "no valid logs to append", http.StatusBadRequest)
			return
		}
		newEntries := newLogs.convertToLogEntries()

		// Append the logs to the node's log
		n.logs.Append(newLogs...)
		slog.InfoContext(ctx, "Logs appended", "logs", newLogs)

		n.mu.Unlock()

		// Replicate the logs to other nodes
		replicatedCount := 1
		for i, node := range n.cluster.OtherNodes(n.id) {
			go func(i int, node *NodeInfo) {
				client := node.RPCClient
				prevLogIndex := uint64(0)
				prevLogTerm := uint64(0)

				n.mu.Lock()
				defer n.mu.Unlock()
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
					Entries:      newEntries,
					LeaderCommit: uint64(n.commitIndex),
				}

				resp, err := client.AppendEntries(ctx, req)
				if err != nil {
					slog.Error("failed to send AppendEntriesRequest", "error", err)
					return
				}

				if resp.Term > uint64(n.currentTerm) {
					slog.InfoContext(ctx,
						"Received higher term during AppendEntriesRequest, stepping down",
						"node_id", n.id,
						"received_term", resp.Term,
						"current_term", n.currentTerm)

					n.becomeFollower(ctx, Term(resp.Term))
					return
				}

				if !resp.Success {
					slog.InfoContext(ctx, "AppendEntriesRequest failed", "node_from", node.ID)

					return
				}

				slog.InfoContext(ctx, "AppendEntriesRequest successful", "node_from", node.ID)
				replicatedCount++

				if n.isOverMajority(replicatedCount) {
					slog.InfoContext(ctx, "Majority replicated, updating commit index", "node_id", n.id)
					n.commitIndex = n.logs.LastIndex()

					if n.commitIndex > n.lastApplied {
						n.applyStateMachine(ctx)
					}
				}
			}(i, node)
		}

		// Respond to the client
		w.WriteHeader(http.StatusOK)
	})
}

func (n *Node) applyStateMachine(ctx context.Context) {
	slog.InfoContext(ctx, "Committing logs to state machine", "node_id", n.id)

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		logEntry := n.logs.FindByIndex(n.lastApplied)
		if logEntry == nil {
			slog.WarnContext(ctx, "Log entry not found for last applied index", "last_applied", n.lastApplied)
			continue
		}

		key := logEntry.Data.Key
		value := logEntry.Data.Value

		slog.InfoContext(ctx,
			"Applying log entry to state machine",
			"node_id", n.id,
			"last_applied", n.lastApplied,
			"key", key,
			"value", value)

		n.stateMachine.Set(key, value)
	}
}
