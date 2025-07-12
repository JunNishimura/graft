package eliben

import (
	"fmt"
	"net/rpc"
	"sync"
)

type Server struct {
	mu sync.Mutex

	peerClients map[int]*rpc.Client
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	}
	return peer.Call(serviceMethod, args, reply)
}
