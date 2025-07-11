package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

var (
	// Addresses of the servers in the cluster.
	addresses = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
		"localhost:8083",
		"localhost:8084",
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

var leaderAddress string

func main() {
	// Create a new HTTP client
	client := &http.Client{}

	scanner := bufio.NewScanner(os.Stdin)
	slog.Info("client started, waiting for input...")
	for scanner.Scan() {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		if input == "exit" || input == "quit" {
			slog.Info("client exiting")
			os.Exit(0)
		}

		slog.Info("received input", "input", input)

		data := make(map[string]string)
		splitInput := strings.SplitN(input, "=", 2)
		if len(splitInput) != 2 {
			slog.Error("invalid key-value format", "arg", input)
			os.Exit(1)
		}
		key := splitInput[0]
		value := splitInput[1]
		data[key] = value

		// Convert the data to JSON format
		jsonData, err := json.Marshal(data)
		if err != nil {
			slog.Error("failed to marshal data to JSON", "error", err)
			os.Exit(1)
		}

		slog.Info("sending request to Raft cluster", "data", string(jsonData))
		// Send the request to the Raft cluster
		if err := sendRequest(context.Background(), client, jsonData); err != nil {
			slog.Error("failed to send request", "error", err)
			os.Exit(1)
		}
	}
}

func sendRequest(ctx context.Context, client *http.Client, data []byte) error {
	for {
		address := leaderAddress
		if leaderAddress == "" {
			address = addresses[0]
		}

		slog.Info("sending request to leader", "address", address)
		resp, err := sendRequestToLeader(ctx, client, address, data)
		if err != nil {
			slog.Error("failed to send request to leader", "error", err)
			return fmt.Errorf("failed to send request to leader: %w", err)
		}

		if resp.StatusCode == http.StatusOK {
			slog.Info("request sent successfully", "status", resp.Status)
			return nil
		}

		if resp.StatusCode == http.StatusForbidden {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				slog.Error("failed to read response body", "error", err)
				return fmt.Errorf("failed to read response body: %w", err)
			}
			var response struct {
				LeaderID      string `json:"leader_id"`
				LeaderAddress string `json:"leader_address"`
			}
			if err := json.Unmarshal(body, &response); err != nil {
				slog.Error("failed to unmarshal response", "error", err)
				return fmt.Errorf("failed to unmarshal response: %w", err)
			}
			slog.Info("received redirect to leader", "leader_id", response.LeaderID, "leader_address", response.LeaderAddress)
			leaderAddress = response.LeaderAddress

			slog.Info("retrying request to new leader", "leaderAddress", leaderAddress)
			resp, err = sendRequestToLeader(ctx, client, leaderAddress, data)
			if err != nil {
				slog.Error("failed to send request to new leader", "error", err)
				return fmt.Errorf("failed to send request to new leader: %w", err)
			}
			if resp.StatusCode == http.StatusOK {
				slog.Info("request sent successfully to new leader", "status", resp.Status)
				return nil
			}

			slog.Error("unexpected status code from new leader", "status", resp.Status)
			return fmt.Errorf("unexpected status code from new leader: %s", resp.Status)
		}
	}
}

func sendRequestToLeader(ctx context.Context, client *http.Client, address string, data []byte) (*http.Response, error) {
	slog.Info("sending request to leader", "address", address)
	url := "http://" + address + "/raft/append"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		slog.Error("failed to create request", "error", err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("failed to send request", "error", err)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	return resp, nil
}
