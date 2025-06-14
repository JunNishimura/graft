package main

import (
	"net"
	"net/http"

	"github.com/JunNishimura/graft/raft"
	raftpb "github.com/JunNishimura/graft/raft/grpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const serverCount = 3

func main() {
	addresses := []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}

	var g errgroup.Group

	for i := 0; i < serverCount; i++ {
		listner, err := net.Listen("tcp", addresses[i])
		if err != nil {
			panic(err)
		}
		defer listner.Close()

		server := grpc.NewServer()
		raftpb.RegisterGreetingServiceServer(server, raft.NewGreetingServiceServer())

		reflection.Register(server)

		g.Go(func() error {
			if err := server.Serve(listner); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}
}
