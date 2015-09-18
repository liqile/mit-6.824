package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrEmptyKey     = "ErrEmptyKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrBackupFaild  = "ErrBackupFaild"
	ErrDuplicateKey = "ErrDuplicateKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	SerailNumber int64
	Client       string
	Type         string

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type PBSynArgs struct {
	Data    map[string]string
	Primary string
}

type PBSynReply struct {
	Err Err
}

type PDArgs struct {
}
type PDReply struct {
}

const PutTag = "Put"
const AppendTag = "Append"
