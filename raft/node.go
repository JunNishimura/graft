package raft

import (
	"context"
	"log"
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

type Node struct {
	id              ID
	electionTimeout time.Duration
	state           State
	currentTerm     Term
	votedFor        *ID
	logs            Logs

	clients []raftpb.RaftServiceClient
	raftpb.UnimplementedRaftServiceServer
}

const (
	electionTimeoutLowerBound = 150 * time.Millisecond
	electionTimeoutUpperBound = 300 * time.Millisecond
)

var (
	_ raftpb.RaftServiceServer = (*Node)(nil)
)

func NewNode(clients []raftpb.RaftServiceClient) *Node {
	return &Node{
		id: NewID(),
		electionTimeout: rand.GenerateDuration(
			electionTimeoutLowerBound,
			electionTimeoutUpperBound,
		),
		state:   StateFollower,
		clients: clients,
	}
}

func (n *Node) Run(ctx context.Context) error {
	for {
		timer := time.NewTimer(n.electionTimeout)

		select {
		case <-timer.C:
			n.runForLeader(ctx)
		}
	}

	return nil
}

func (n *Node) runForLeader(ctx context.Context) error {
	// Election timeout reached, start a new election
	n.state = StateCandidate
	n.currentTerm++
	n.votedFor = &n.id

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

	voteCount := 1
	for resp := range respCh {
		if resp.Term > uint64(n.currentTerm) {
			n.loseElection(resp.Term)
			close(respCh)
		}

		if !resp.VoteGranted {
			continue
		}

		voteCount++
		if n.isElectionWinner(voteCount) {
			n.winElection()
			close(respCh)
		}
	}

	return nil
}

func (n *Node) isElectionWinner(voteCount int) bool {
	// If the node has received more than half of the votes, it is the leader
	return voteCount > len(n.clients)/2
}

func (n *Node) winElection() {
	n.state = StateLeader
	n.votedFor = nil
}

func (n *Node) loseElection(term uint64) {
	n.state = StateFollower
	n.votedFor = nil
	n.currentTerm = Term(term)
}

func (n *Node) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	// check if the request term is greater than or equal to the current term.
	if !n.currentTerm.IsVotable(Term(req.Term)) {
		return &raftpb.RequestVoteResponse{
			Term:        uint64(n.currentTerm),
			VoteGranted: false,
		}, nil
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

	return &raftpb.RequestVoteResponse{
		Term:        uint64(n.currentTerm),
		VoteGranted: true,
	}, nil
}
