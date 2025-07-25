package eliben

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/JunNishimura/graft/eliben/kvclient"
	"github.com/JunNishimura/graft/eliben/kvservice"
	"github.com/JunNishimura/graft/eliben/raft"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	n int

	kvCluster      []*kvservice.KVService
	kvServiceAddrs []string

	storage []*raft.MapStorage

	t *testing.T

	connected []bool

	alive []bool

	ctx       context.Context
	ctxCancel func()
}

func NewHarness(t *testing.T, n int) *Harness {
	kvss := make([]*kvservice.KVService, n)
	ready := make(chan any)
	connected := make([]bool, n)
	alive := make([]bool, n)
	storage := make([]*raft.MapStorage, n)

	for i := range n {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = raft.NewMapStorage()
		kvss[i] = kvservice.New(i, peerIds, storage[i], ready)
		alive[i] = true
	}

	for i := range n {
		for j := range n {
			if i != j {
				kvss[i].ConnectToRaftPeer(j, kvss[j].GetRaftListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	kvServiceAddrs := make([]string, n)
	for i := range n {
		port := 14200 + i
		kvss[i].ServeHTTP(port)

		kvServiceAddrs[i] = fmt.Sprintf("localhost:%d", port)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	return &Harness{
		n:              n,
		kvCluster:      kvss,
		kvServiceAddrs: kvServiceAddrs,
		t:              t,
		connected:      connected,
		alive:          alive,
		storage:        storage,
		ctx:            ctx,
		ctxCancel:      ctxCancel,
	}
}

func (h *Harness) Shutdown() {
	for i := range h.n {
		h.kvCluster[i].DisconnectFromAllRaftPeers()
		h.connected[i] = false
	}

	http.DefaultClient.CloseIdleConnections()
	h.ctxCancel()

	for i := range h.n {
		if h.alive[i] {
			h.alive[i] = false
			if err := h.kvCluster[i].Shutdown(); err != nil {
				h.t.Errorf("error while shutting down service %d: %v", i, err)
			}
		}
	}
}

func (h *Harness) NewClient() *kvclient.KVClient {
	var addrs []string
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	return kvclient.New(addrs)
}
func (h *Harness) NewClientSingleService(id int) *kvclient.KVClient {
	addrs := h.kvServiceAddrs[id : id+1]
	return kvclient.New(addrs)
}

func (h *Harness) NewClientWithRandomAddrsOrder() *kvclient.KVClient {
	var addrs []string
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return kvclient.New(addrs)
}

func (h *Harness) DisconnectServiceFromPeers(id int) {
	tlog("Disconnect %d", id)
	h.kvCluster[id].DisconnectFromAllRaftPeers()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.kvCluster[j].DisconnectFromRaftPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) ReconnectServiceToPeers(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.kvCluster[id].ConnectToRaftPeer(j, h.kvCluster[j].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.kvCluster[j].ConnectToRaftPeer(id, h.kvCluster[id].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

func (h *Harness) CrashService(id int) {
	tlog("Crash %d", id)
	h.DisconnectServiceFromPeers(id)
	h.alive[id] = false
	if err := h.kvCluster[id].Shutdown(); err != nil {
		h.t.Errorf("error while shutting down service %d: %v", id, err)
	}
}

func (h *Harness) RestartService(id int) {
	if h.alive[id] {
		log.Fatalf("service %d is already alive", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}
	ready := make(chan any)
	h.kvCluster[id] = kvservice.New(id, peerIds, h.storage[id], ready)
	h.kvCluster[id].ServeHTTP(14200 + id)

	h.ReconnectServiceToPeers(id)
	close(ready)
	h.alive[id] = true
	time.Sleep(20 * time.Millisecond) // Give some time for the service to stabilize
}

func (h *Harness) DelayNextHTTPResponseFromService(id int) {
	tlog("Delaying next HTTP response from %d", id)
	h.kvCluster[id].DelayNextHTTPResponse()
}

func (h *Harness) CheckSingleLeader() int {
	for r := 0; r < 8; r++ {
		leaderId := -1
		for i := range h.n {
			if h.connected[i] && h.kvCluster[i].IsLeader() {
				if leaderId < 0 {
					leaderId = i
				} else {
					h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
				}
			}
		}
		if leaderId >= 0 {
			return leaderId
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1
}

func (h *Harness) CheckPut(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, err := c.Put(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func (h *Harness) CheckAppend(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, err := c.Append(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func (h *Harness) CheckGet(c *kvclient.KVClient, key, expectedValue string) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	gv, f, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}
	if !f {
		h.t.Errorf("got found=false, want true for key=%s", key)
	}
	if gv != expectedValue {
		h.t.Errorf("got value=%v, want %v", gv, expectedValue)
	}
}

func (h *Harness) CheckGetTimeout(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(h.ctx, 300*time.Millisecond)
	defer cancel()
	_, _, err := c.Get(ctx, key)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		h.t.Errorf("expected timeout error for key=%s, got nil", key)
	}
}

func (h *Harness) CheckCAS(c *kvclient.KVClient, key, compare, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, err := c.CAS(ctx, key, compare, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
