package raftkv

import (
	"MapReduce/6.824/src/labrpc"
	"MapReduce/6.824/src/raft"
	"encoding/gob"
	"time"
	"log"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string // 代表单个操作的字符串Get，Put，Append
	ClientID  int64  // 每个Client的ID
	Clientseq int64  // 这个ClientID上目前的操作数
	// 之所以选择这样而不是每一个操作都给一个序列号是为了减小存储空间
}

type LatestReply struct {
	Seq   int64
	Value string // 之所以get不直接从db中取是因为取时的最新值不一定是读时的最新值，我们需要一个严格有序的操作序列
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ClientSeqCache map[int64]*LatestReply // 用作判断当前请求是否已经执行过
	LogIndexNotice map[int]chan struct{}  // 用作在每一个函数中等待对应日志项提交
	Shutdown       chan struct{}          // 用作结束算法
	KvDictionary   map[string]string      // 存储所有的数据
}

/*
 * 1. 不是leader的话拒绝请求，并返回错误消息
 * 2.
 */

func StopTimer(arg *time.Timer) {
	if arg != nil {
		arg.Stop()
	}
}

// Q: 有一个匪夷所思的bug,就是调用get的时候会服务器阻塞，现在在Client中加一个超时重传
// A: 这个匪夷所思的bug出现在分区的时候，所以不难解释了，因为我没有开始加超时，所以这个分区永远恢复不了
// 而且超时时间的选择一定要远长于一次正常的交互时长，否则每次重试都会启动一个定时器，发很多的重复操作
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Printf("%d server: Get: %q, seq is %d\n",args.ClientID,args.Key,args.Clientseq)
	if _, IsLeader := kv.rf.GetState(); !IsLeader {
		//fmt.Println("server no leader")
		reply.Err = NoLeader
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if rep, ok := kv.ClientSeqCache[args.ClientID]; ok { // 重复请求
		if args.Clientseq <= rep.Seq {
			//fmt.Printf("重复请求 get %d %d\n",args.Clientseq,rep.Seq)
			kv.mu.Unlock()
			reply.Value = rep.Value
			reply.WrongLeader = false
			reply.Err = Duplicate
			// 这里返回的原因是第一个发送的请求可能未被回复（切换leader），但是确实已经提交，所以需要返回值，尽管知道它是重复的
			return
		}
	}
	// 如果得到回复，即这条日志被提交，根据日志匹配原则，可以保证看到的数据是最新的
	NewOperation := Op{Key: args.Key, Op: "Get", ClientID: args.ClientID, Clientseq: args.Clientseq}
	index, term, _ := kv.rf.Start(NewOperation)
	Notice := make(chan struct{})
	//fmt.Printf("此get插入的日志index为%d\n",index)
	kv.LogIndexNotice[index] = Notice
	kv.mu.Unlock()
	reply.WrongLeader = false
	reply.Err = OK

	//fmt.Printf("上面 idnex : %d; term %d\n", index, term)
	select {
	case <-kv.Shutdown:
		return
	case <-Notice:
		CurrentTerm, Isleader := kv.rf.GetState()
		if !Isleader || term != CurrentTerm {
			reply.WrongLeader = true
			reply.Err = ReElection // 可能在提交之前重选也可能提交之后重选，所以需要重新发送
			return
		}
		kv.mu.Lock()
		if value, ok := kv.KvDictionary[args.Key]; ok { // 向客户端返回数据
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = "" // 这样写client.go可以少一个条件语句
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, IsLeader := kv.rf.GetState(); !IsLeader {
		reply.Err = NoLeader
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock() // 这部分数据会被并发访问

	if rep, ok := kv.ClientSeqCache[args.ClientID]; ok { // 检测请求是否重复
		//fmt.Printf("putappend %d %d\n",args.Clientseq,rep.Seq)
		if args.Clientseq <= rep.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = Duplicate
			return
		}
	}
	// 提交并等待
	reply.WrongLeader = false
	reply.Err = OK
	NewOperation := Op{args.Key, args.Value, args.Op, args.ClientID, args.Clientseq}
	index, term, _ := kv.rf.Start(NewOperation) // 当做一个日志传入Raft，返回这条命令的日志index和当前的term
	// 此时我们需要一个通知机制，在这个日志被提交的时候通知我们，但是不能直接观测applyCh，因为PutAppend可能会并发调用
	Notice := make(chan struct{})
	kv.LogIndexNotice[index] = Notice
	kv.mu.Unlock()

	select {
	case <-kv.Shutdown:
		return
	case <-Notice: // 管道被关闭时所有的请求从管道输出都会关闭
		CurrentTerm, Isleader := kv.rf.GetState()
		if !Isleader || term != CurrentTerm {
			reply.WrongLeader = true
			reply.Err = ReElection // 可能在提交之前重选也可能提交之后重选，所以需要重新发送
		}
		return
	}
}

// 用作一个守护进程，负责把从applyCh中接收到的命令转化成数据库中的值。
// 并在接收到命令的同时通知请求上的channel用于向客户返回数据
func (kv *RaftKV) DealWithapplyCh() {
	for {
		select {
		case <-kv.Shutdown:
			DPrintf("[%d]: server %d is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh: // 当数据被提交以后就从applyCh中被返回
			if ok {
				cmd := msg.Command.(Op)
				kv.mu.Lock()
				// 这个新到的请求的序列必须大于seq，否则就是重复的
				if rep, ok := kv.ClientSeqCache[cmd.ClientID]; !ok || rep.Seq < cmd.Clientseq {
					switch cmd.Op {
					case "Put":
						kv.KvDictionary[cmd.Key] = cmd.Value
						kv.ClientSeqCache[cmd.ClientID] = &LatestReply{Seq: cmd.Clientseq}
						//fmt.Printf("put数据成功 %q：%q \n",cmd.Key,cmd.Value)
					case "Append":
						kv.KvDictionary[cmd.Key] = kv.KvDictionary[cmd.Key] + cmd.Value
						kv.ClientSeqCache[cmd.ClientID] = &LatestReply{Seq: cmd.Clientseq}
						//fmt.Printf("append数据成功 %q：%q \n",cmd.Key,cmd.Value)
					case "Get":
						kv.ClientSeqCache[cmd.ClientID] = &LatestReply{Seq: cmd.Clientseq, Value: kv.KvDictionary[cmd.Key]}
						//fmt.Printf("nihao\n")
					default:
						panic("Invalid Operation. Please check all cmd!")
					}
				}
				if Notice, ok := kv.LogIndexNotice[msg.Index]; ok && Notice != nil {
					close(Notice)
					delete(kv.LogIndexNotice, msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ClientSeqCache = make(map[int64]*LatestReply)
	kv.LogIndexNotice = make(map[int]chan struct{})
	kv.Shutdown = make(chan struct{})
	kv.KvDictionary = make(map[string]string)

	go kv.DealWithapplyCh()
	return kv
}
