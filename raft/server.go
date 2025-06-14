package raft

import (
	"context"
	"fmt"

	raftpb "github.com/JunNishimura/graft/raft/grpc"
)

type greetingServiceServer struct {
	raftpb.UnimplementedGreetingServiceServer
}

func NewGreetingServiceServer() raftpb.GreetingServiceServer {
	return &greetingServiceServer{}
}

func (s *greetingServiceServer) Hello(ctx context.Context, req *raftpb.HelloRequest) (*raftpb.HelloResponse, error) {
	return &raftpb.HelloResponse{
		Message: fmt.Sprintf("Hello, %s!", req.GetName()),
	}, nil
}
