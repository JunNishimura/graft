package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
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

		server := http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "Hello from server %d at %s", i+1, addresses[i])
			}),
			ReadTimeout: 30 * time.Second,
		}

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
