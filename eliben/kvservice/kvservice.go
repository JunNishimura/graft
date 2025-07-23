package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/JunNishimura/graft/eliben/api"
	"github.com/JunNishimura/graft/eliben/raft"
)

type KVService struct {
	sync.Mutex

	id int

	rs *raft.Server

	commitChan chan raft.CommitEntry

	commitSubs map[int]chan Command

	ds *DataStore

	srv *http.Server

	lastRequestIDPerClient map[int64]int64

	httpResponsesEnabled bool
}

func New(id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
	rs.Serve()
	kvs := &KVService{
		id:                   id,
		rs:                   rs,
		commitChan:           commitChan,
		ds:                   NewDataStore(),
		commitSubs:           make(map[int]chan Command),
		httpResponsesEnabled: true,
	}

	kvs.runUpdater()
	return kvs
}

func (kvs *KVService) Shutdown() error {
	kvs.kvlog("shutting down Raft server")
	kvs.rs.Shutdown()
	kvs.kvlog("closing commit channel")
	close(kvs.commitChan)

	if kvs.srv != nil {
		kvs.kvlog("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kvs.srv.Shutdown(ctx)
		kvs.kvlog("HTTP shutdown complete")
		return nil
	}

	return nil
}

func (kvs *KVService) IsLeader() bool {
	return kvs.rs.IsLeader()
}

func (kvs *KVService) ServeHTTP(port int) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)
	mux.HandleFunc("POST /append/", kvs.handleAppend)
	mux.HandleFunc("POST /cas/", kvs.handleCAS)

	kvs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		kvs.kvlog("serving HTTP on %s", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

func (kvs *KVService) sendHTTPResponse(w http.ResponseWriter, v any) {
	if kvs.httpResponsesEnabled {
		renderJSON(w, v)
	}
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {
	gr := &api.GetRequest{}
	if err := readRequestJSON(req, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP GET %v", gr)

	cmd := Command{
		Kind:      CommandGet,
		Key:       gr.Key,
		ServiceID: kvs.id,
		ClientID:  gr.ClientID,
		RequestID: gr.RequestID,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.GetResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					Value:      commitCmd.ResultValue,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	pr := &api.PutRequest{}
	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP PUT %v", pr)

	cmd := Command{
		Kind:      CommandPut,
		Key:       pr.Key,
		Value:     pr.Value,
		ServiceID: kvs.id,
		ClientID:  pr.ClientID,
		RequestID: pr.RequestID,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.PutResponse{
					RespStatus: api.StatusOK,
					PrevValue:  commitCmd.ResultValue,
					KeyFound:   commitCmd.ResultFound,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleAppend(w http.ResponseWriter, req *http.Request) {
	ar := &api.AppendRequest{}
	if err := readRequestJSON(req, ar); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP APPEND %v", ar)

	cmd := Command{
		Kind:      CommandAppend,
		Key:       ar.Key,
		Value:     ar.Value,
		ServiceID: kvs.id,
		ClientID:  ar.ClientID,
		RequestID: ar.RequestID,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusNotLeader})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, req *http.Request) {
	cr := &api.CASRequest{}
	if err := readRequestJSON(req, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP CAS %v", cr)

	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		CompareValue: cr.CompareValue,
		Value:        cr.Value,
		ServiceID:    kvs.id,
		ClientID:     cr.ClientID,
		RequestID:    cr.RequestID,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == kvs.id {
			if commitCmd.IsDuplicate {
				kvs.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				kvs.sendHTTPResponse(w, api.CASResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			kvs.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusNotLeader})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) runUpdater() {
	go func() {
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			lastReqID, ok := kvs.lastRequestIDPerClient[cmd.ClientID]
			if ok && cmd.RequestID <= lastReqID {
				kvs.kvlog("duplicate request id=%v, from client id=%v", cmd.RequestID, cmd.ClientID)
				cmd = Command{
					Kind:        cmd.Kind,
					IsDuplicate: true,
				}
			} else {
				kvs.lastRequestIDPerClient[cmd.ClientID] = cmd.RequestID

				switch cmd.Kind {
				case CommandGet:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
				case CommandPut:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
				case CommandAppend:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.Append(cmd.Key, cmd.Value)
				case CommandCAS:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
				default:
					panic(fmt.Errorf("unexpected command %v", cmd))
				}
			}

			if sub := kvs.popCommitSubscription(entry.Index); sub != nil {
				sub <- cmd
				close(sub)
			}
		}
	}()
}

func (kvs *KVService) createCommitSubscription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	if _, exists := kvs.commitSubs[logIndex]; exists {
		panic(fmt.Sprintf("duplicate commit subscription for log index %d", logIndex))
	}

	ch := make(chan Command, 1)
	kvs.commitSubs[logIndex] = ch
	return ch
}

func (kvs *KVService) popCommitSubscription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	ch := kvs.commitSubs[logIndex]
	delete(kvs.commitSubs, logIndex)
	return ch
}

const DebugKV = 1

func (kvs *KVService) kvlog(format string, args ...any) {
	if DebugKV > 0 {
		format = fmt.Sprintf("[kv %d] ", kvs.id) + format
		log.Printf(format, args...)
	}
}

func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) DisconnectFromAllRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) DisconnectFromRaftPeer(peerId int) error {
	return kvs.rs.DisconnectPeer(peerId)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}
