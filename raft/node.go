package raft

import (
	"context"
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

	raftpb.UnimplementedRaftServiceServer
}

const (
	electionTimeoutLowerBound = 150 * time.Millisecond
	electionTimeoutUpperBound = 300 * time.Millisecond
)

var (
	_ raftpb.RaftServiceServer = (*Node)(nil)
)

func NewNode() *Node {
	return &Node{
		electionTimeout: rand.GenerateDuration(
			electionTimeoutLowerBound,
			electionTimeoutUpperBound,
		),
		state: StateFollower,
	}
}

func (n *Node) Run() error {
	if err := n.Serve(); err != nil {
		return err
	}
	if err := n.Elect(); err != nil {
		return err
	}
	return nil
}

func (n *Node) Serve() error {
	return nil
}

func (n *Node) Elect() error {
	return nil
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
