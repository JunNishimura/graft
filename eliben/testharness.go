package eliben

import (
	"log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu sync.Mutex

	cluster []*Server
	storage []*MapStorage

	commitChans []chan CommitEntry

	commits [][]CommitEntry

	connected []bool

	alive []bool

	n int
	t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	ready := make(chan any)
	storage := make([]*MapStorage, n)

	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = NewMapStorage()
		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewServer(i, peerIds, storage[i], ready, commitChans[i])
		ns[i].Serve()
		alive[i] = true
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

	h := &Harness{
		cluster:     ns,
		storage:     storage,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		alive:       alive,
		n:           n,
		t:           t,
	}
	for i := 0; i < n; i++ {
		go h.collectCommits(i)
	}
	return h
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			h.cluster[i].Shutdown()
		}
	}
	for i := 0; i < h.n; i++ {
		close(h.commitChans[i])
	}
}

func (h *Harness) PeerDropCallsAfterN(id int, n int) {
	tlog("peer %d drop calls after %d", id, n)
	h.cluster[id].Proxy().DropCallsAfterN(n)
}

func (h *Harness) PeerDontDropCalls(id int) {
	tlog("peer %d don't drop calls", id)
	h.cluster[id].Proxy().DontDropCalls()
}

func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	h.DisconnectPeer(id)
	h.alive[id] = false
	h.cluster[id].Shutdown()

	h.mu.Lock()
	h.commits[id] = h.commits[id][:0]
	h.mu.Unlock()
}

func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan any)
	h.cluster[id] = NewServer(id, peerIds, h.storage[id], ready, h.commitChans[id])
	h.cluster[id].Serve()
	h.ReconnectPeer(id)
	close(ready)
	h.alive[id] = true
	sleepMs(20)
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
		if j != peerId && h.alive[j] {
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

func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	commitsLen := -1
	for i := 0; i < h.n; i++ {
		if !h.connected[i] {
			continue
		}
		if commitsLen >= 0 {
			if len(h.commits[i]) != commitsLen {
				h.t.Errorf("commits[%d] = %d, commitsLen = %d", i, h.commits[i], commitsLen)
			}
		} else {
			commitsLen = len(h.commits[i])
		}
	}

	for c := 0; c < commitsLen; c++ {
		cmdAtC := -1
		for i := 0; i < h.n; i++ {
			if !h.connected[i] {
				continue
			}
			cmdOfN := h.commits[i][c].Command.(int)
			if cmdAtC >= 0 {
				if cmdAtC != cmdOfN {
					h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, i, c)
				}
			} else {
				cmdAtC = cmdOfN
			}
		}
		if cmdAtC == cmd {
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if !h.connected[i] {
					continue
				}
				if index >= 0 && h.commits[i][c].Index != index {
					h.t.Errorf("got index=%d, want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
				} else {
					index = h.commits[i][c].Index
				}
				nc++
			}
			return nc, index
		}
	}

	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

func (h *Harness) CheckCommittedN(cmd int, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

func (h *Harness) CheckNotCommitted(cmd int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.n; i++ {
		if !h.connected[i] {
			continue
		}
		for c := 0; c < len(h.commits[i]); c++ {
			gotCmd := h.commits[i][c].Command.(int)
			if gotCmd == cmd {
				h.t.Errorf("found %d at commits[%d][%d], expected none", cmd, i, c)
			}
		}
	}
}

func (h *Harness) SubmitToServer(serverId int, command any) int {
	return h.cluster[serverId].cm.Submit(command)
}

func tlog(format string, a ...any) {
	format = "[TEST]" + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func (h *Harness) collectCommits(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}
