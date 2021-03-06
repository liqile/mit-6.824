package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

const (
	GetFlag    = "Get"
	PutFlag    = "Put"
	AppendFlag = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Uid   string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Uid string
}

type GetReply struct {
	Err   Err
	Value string
}
