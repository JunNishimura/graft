package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/JunNishimura/graft/raft"
	raftpb "github.com/JunNishimura/graft/raft/grpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	// Addresses of the servers in the cluster.
	addresses = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}
)

func init() {
	setupLogging()
}

func setupLogging() {
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}

func main() {
	var g errgroup.Group

	cluster, cleanup, err := raft.NewCluster(addresses)
	if err != nil {
		slog.Error("failed to create cluster", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := cleanup(); err != nil {
			slog.Error("failed to cleanup cluster", "error", err)
			os.Exit(1)
		}
	}()

	for _, node := range cluster.Nodes() {
		listner, err := net.Listen("tcp", node.Address)
		if err != nil {
			slog.Error("failed to listen on address", "address", node.Address, "error", err)
			os.Exit(1)
		}
		defer listner.Close()

		server := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(server, raft.NewNode(node.ID, cluster))

		reflection.Register(server)

		g.Go(func() error {
			if err := server.Serve(listner); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		slog.Error("failed to wait for server", "error", err)
		os.Exit(1)
	}
}
