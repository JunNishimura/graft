package raft

import (
	"fmt"

	raftpb "github.com/JunNishimura/graft/mine/raft/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeInfo struct {
	ID        ID
	Address   string
	RPCClient raftpb.RaftServiceClient
}

type Cluster map[ID]*NodeInfo

func NewCluster(addresses []string) (Cluster, func() error, error) {
	conns := make([]*grpc.ClientConn, 0, len(addresses))

	cluster := make(Cluster, len(addresses))
	for _, address := range addresses {
		nodeID := NewID()

		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to connect to %s: %w", address, err)
		}
		conns = append(conns, conn)

		cluster[nodeID] = &NodeInfo{
			ID:        nodeID,
			Address:   address,
			RPCClient: raftpb.NewRaftServiceClient(conn),
		}
	}
	return cluster, func() error {
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				return fmt.Errorf("failed to close connection: %w", err)
			}
		}
		return nil
	}, nil
}

func (c Cluster) Nodes() []*NodeInfo {
	nodes := make([]*NodeInfo, 0, len(c))
	for _, node := range c {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c Cluster) OtherNodes(nodeID ID) []*NodeInfo {
	otherNodes := make([]*NodeInfo, 0, len(c)-1)
	for id, node := range c {
		if !id.Equal(nodeID) {
			otherNodes = append(otherNodes, node)
		}
	}
	return otherNodes
}

func (c Cluster) NodeCount() int {
	return len(c)
}
