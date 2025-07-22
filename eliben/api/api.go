package api

type PutRequest struct {
	Key   string
	Value string
}

type Response interface {
	Status() ResponseStatus
}

type PutResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (r *PutResponse) Status() ResponseStatus {
	return r.RespStatus
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	Value      string
}

func (gr *GetResponse) Status() ResponseStatus {
	return gr.RespStatus
}

type CASRequest struct {
	Key          string
	CompareValue string
	Value        string
}

type CASResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (cr *CASResponse) Status() ResponseStatus {
	return cr.RespStatus
}

type AppendRequest struct {
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type AppendResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (ar *AppendResponse) Status() ResponseStatus {
	return ar.RespStatus
}

type ResponseStatus int

const (
	StatusInvalid ResponseStatus = iota
	StatusOK
	StatusNotLeader
	StatusFailedCommit
	StatusDuplicateRequest
)

var responseName = map[ResponseStatus]string{
	StatusInvalid:          "invalid",
	StatusOK:               "OK",
	StatusNotLeader:        "NotLeader",
	StatusFailedCommit:     "FailedCommit",
	StatusDuplicateRequest: "DuplicateRequest",
}

func (rs ResponseStatus) String() string {
	return responseName[rs]
}
