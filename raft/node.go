package raft

import (
	"time"

	"github.com/JunNishimura/go-template/rand"
)

type Node struct {
	electionTimeout time.Duration
	state           State
	logs            []byte
}

const (
	electionTimeoutLowerBound = 150 * time.Millisecond
	electionTimeoutUpperBound = 300 * time.Millisecond
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
