package eliben

import (
	"log"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	cluster []*Server

	connected []bool

	n int
	t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	ready := make(chan any)

	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		ns[i] = NewServer(i, peerIds, ready)
		ns[i].Serve()
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	return &Harness{
		cluster:   ns,
		connected: connected,
		n:         n,
		t:         t,
	}
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
}

func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if !h.connected[i] {
				continue
			}
			_, term, isLeader := h.cluster[i].cm.Report()
			if isLeader {
				if leaderId < 0 {
					leaderId = i
					leaderTerm = term
				} else {
					h.t.Fatalf("both %d and %d are leaders in term %d", leaderId, i, term)
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if !h.connected[i] {
			continue
		}
		_, _, isLeader := h.cluster[i].cm.Report()
		if isLeader {
			h.t.Fatalf("server %d leader; want none", i)
		}
	}
}

func (h *Harness) DisconnectPeer(peerId int) {
	tlog("Disconnect %d", peerId)
	h.cluster[peerId].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != peerId {
			h.cluster[j].DisconnectPeer(peerId)
		}
	}
	h.connected[peerId] = false
}

func (h *Harness) ReconnectPeer(peerId int) {
	tlog("Reconnect %d", peerId)
	for j := 0; j < h.n; j++ {
		if j != peerId {
			if err := h.cluster[peerId].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(peerId, h.cluster[peerId].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[peerId] = true
}

func tlog(format string, a ...any) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
