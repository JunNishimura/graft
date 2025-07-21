package eliben

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func TestSetupHarness(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	sleepMs(80)
}

func TestClientRequestBeforeConsensus(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	sleepMs(10)

	c1 := h.NewClient()
	h.CheckPut(c1, "llave", "cosa")
	sleepMs(80)
}

func TestBasicPutGetSingleClient(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "llave", "cosa")

	h.CheckGet(c1, "llave", "cosa")

	sleepMs(80)
}

func TestPutPrevValue(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	prev, found := h.CheckPut(c1, "llave", "cosa")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%q, want false/""`, found, prev)
	}

	prev, found = h.CheckPut(c1, "llave", "frodo")
	if !found || prev != "cosa" {
		t.Errorf(`got found=%v, prev=%q, want true/"cosa"`, found, prev)
	}

	prev, found = h.CheckPut(c1, "maftech", "davar")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%q, want true/""`, found, prev)
	}
}

func TestBasicPutGetDifferentClients(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "k", "v")

	c2 := h.NewClient()
	h.CheckGet(c2, "k", "v")
	sleepMs(80)
}

func TestCASBasic(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "k", "v")

	if pv, found := h.CheckCAS(c1, "k", "v", "newv"); pv != "v" || !found {
		t.Errorf("CheckCAS got prev=%q, found=%v, want prev=v, found=true", pv, found)
	}
}

func TestCASConcurrent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()
	c := h.NewClient()
	h.CheckPut(c, "foo", "mexico")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := h.NewClient()
		for range 20 {
			h.CheckCAS(c, "foo", "bar", "bomba")
		}
	}()

	sleepMs(50)
	c2 := h.NewClient()
	h.CheckPut(c2, "foo", "bar")

	sleepMs(300)
	h.CheckGet(c, "foo", "bomba")

	wg.Wait()
}

func TestConcurrentClientsPutsAndGets(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	n := 9
	for i := range n {
		go func(i int) {
			c := h.NewClient()
			_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
			if f {
				t.Errorf("got key found for %d, want false", i)
			}
		}(i)
	}
	sleepMs(150)

	for i := range n {
		go func(i int) {
			c := h.NewClient()
			h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		}(i)
	}
	sleepMs(150)
}

func Test5ServerConcurrentClientsPutsAndGets(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()
	h.CheckSingleLeader()

	n := 9
	for i := range n {
		go func(i int) {
			c := h.NewClient()
			_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
			if f {
				t.Errorf("got key found for %d, want false", i)
			}
		}(i)
	}
	sleepMs(150)

	for i := range n {
		go func(i int) {
			c := h.NewClient()
			h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		}(i)
	}
	sleepMs(150)
}

func TestDisconnectLeaderAfterPuts(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 4
	for i := range n {
		c := h.NewClient()
		h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.DisconnectServiceFromPeers(lid)
	sleepMs(300)
	newlid := h.CheckSingleLeader()

	if newlid == lid {
		t.Errorf("got the same leader")
	}

	c := h.NewClientSingleService(lid)
	h.CheckGetTimeout(c, "key1")

	for range 5 {
		c := h.NewClientWithRandomAddrsOrder()
		for j := range n {
			h.CheckGet(c, fmt.Sprintf("key%v", j), fmt.Sprintf("value%v", j))
		}
	}

	h.ReconnectServiceToPeers(lid)
	sleepMs(200)
}

func TestDisconnectLeaderAndFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 4
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	h.DisconnectServiceFromPeers(lid)
	otherId := (lid + 1) % h.n
	h.DisconnectServiceFromPeers(otherId)
	sleepMs(100)

	c := h.NewClient()
	h.CheckGetTimeout(c, "key0")

	h.ReconnectServiceToPeers(otherId)
	h.CheckSingleLeader()
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.ReconnectServiceToPeers(lid)
	h.CheckSingleLeader()
	for i := range n {
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
	sleepMs(100)
}

func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 3
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	otherId := (lid + 1) % h.n
	h.CrashService(otherId)

	for i := range n {
		c := h.NewClientSingleService(lid)
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
}

func TestCrashLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 3
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	h.CrashService(lid)
	h.CheckSingleLeader()

	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}
}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()
	lid := h.CheckSingleLeader()

	n := 3
	for i := range n {
		c := h.NewClient()
		_, f := h.CheckPut(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
		if f {
			t.Errorf("got key found for %d, want false", i)
		}
	}

	h.CrashService(lid)
	h.CheckSingleLeader()

	for i := range n {
		c := h.NewClient()
		h.CheckGet(c, fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i))
	}

	h.RestartService(lid)

	for range 5 {
		c := h.NewClientWithRandomAddrsOrder()
		for j := range n {
			h.CheckGet(c, fmt.Sprintf("key%v", j), fmt.Sprintf("value%v", j))
		}
	}
}
