package main

import (
	"context"
	"fmt"
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

	slog.Info("starting Raft cluster", "nodes", cluster.Nodes())

	ctx := context.Background()

	for _, node := range cluster.Nodes() {
		listner, err := net.Listen("tcp", node.Address)
		if err != nil {
			slog.Error("failed to listen on address", "address", node.Address, "error", err)
			os.Exit(1)
		}
		defer listner.Close()

		grpcServer := grpc.NewServer()
		raftNode := raft.NewNode(ctx, node.ID, cluster)
		raftpb.RegisterRaftServiceServer(grpcServer, raftNode)
		reflection.Register(grpcServer)
		g.Go(func() error {
			if err := grpcServer.Serve(listner); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("failed to serve gRPC on %s: %w", node.Address, err)
			}
			return nil
		})

		httpServer := &http.Server{
			Handler: raftNode.HandleClientRequest(ctx),
		}
		g.Go(func() error {
			if err := httpServer.Serve(listner); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("failed to serve HTTP on %s: %w", node.Address, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		slog.Error("failed to wait for server", "error", err)
		os.Exit(1)
	}
}
