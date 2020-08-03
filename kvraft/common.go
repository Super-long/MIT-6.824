package raftkv

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NoLeader   = "NoLeader"
	Duplicate  = "Duplicate"
	ReElection = "ReElection"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	Clientseq int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	Clientseq int64
}

type GetReply struct {
	WrongLeader bool // 个人感觉这个项可以去掉，作用可以用err的一个项代替
	Err         Err
	Value       string
}
