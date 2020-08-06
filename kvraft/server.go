package raftkv

import (
	"MapReduce/6.824/src/labrpc"
	"MapReduce/6.824/src/raft"
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"time"
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
	// 3A
	ClientSeqCache map[int64]*LatestReply // 用作判断当前请求是否已经执行过
	LogIndexNotice map[int]chan struct{}  // 用作在每一个函数中等待对应日志项提交
	Shutdown       chan struct{}          // 用作结束算法
	KvDictionary   map[string]string      // 存储所有的数据

	// 3B
	PersistSnapshots *raft.Persister 	// 用于持久化快照
	SnapshotsIndex int					// 现在日志上哪一个位置以前都已经是快照了，包括这个位置
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
	//fmt.Printf("server get : %s clientseq : %d\n",args.Key, args.Clientseq)
	// Your code here.
	//fmt.Printf("%d server: Get: %q, seq is %d\n",args.ClientID,args.Key,args.Clientseq)
	if _, IsLeader := kv.rf.GetState(); !IsLeader {
		//fmt.Printf("server %d not a leader\n",kv.rf.Getme())
		reply.Err = NoLeader
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if rep, ok := kv.ClientSeqCache[args.ClientID]; ok { // 重复请求 请求未被提交前会继续发送
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
		//fmt.Printf("serverID %d : get已经返回\n",args.ClientID)
		CurrentTerm, Isleader := kv.rf.GetState()
		if !Isleader || term != CurrentTerm {
			//fmt.Printf("isleader : %t, currentTerm : %d, Term : %d\n",Isleader, CurrentTerm, term)
			reply.WrongLeader = true
			reply.Err = ReElection // 可能在提交之前重选也可能提交之后重选，所以需要重新发送
			return
		}
		kv.mu.Lock()
		if value, ok := kv.KvDictionary[args.Key]; ok { // 向客户端返回数据
			reply.Value = value
			//fmt.Println("得到数据 ： " + value)
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

	// TODO 对于重复请求，只需要在commit时更新就好，如果在为commit之前就去重会导致如果消息丢失就没有办法了
	// 其中维护了clientID和seq，所以可以在commit中去重
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
	//fmt.Printf("key : %s\n",args.Key)
	index, term, _ := kv.rf.Start(NewOperation) // 当做一个日志传入Raft，返回这条命令的日志index和当前的term
	// 此时我们需要一个通知机制，在这个日志被提交的时候通知我们，但是不能直接观测applyCh，因为PutAppend可能会并发调用
	index = index - 1	// 返回的index其实是大于1的
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

func (kv *RaftKV) isUpperThanMaxraftstate() bool {
	if kv.maxraftstate < 0 { // 小于-1的时候不执行快照
		return false
	}
	// 后者其实存储的是Raft的状态大小，这个大小在Raft库中是在每次持久化时维护的
	if kv.maxraftstate < kv.PersistSnapshots.RaftStateSize(){
		return true
	}
	// 以上两种是极端情况，我们需要考虑靠近临界值时就持久化快照，暂定15%
	var interval = kv.maxraftstate - kv.PersistSnapshots.RaftStateSize()
	if interval < kv.maxraftstate/20 * 3{
		return true
	}
	return false
}

func (kv *RaftKV) persisterSnapshot(index int){
	Write := new(bytes.Buffer)
	e := gob.NewEncoder(Write)

	kv.SnapshotsIndex = index

	e.Encode(kv.KvDictionary)
	e.Encode(kv.SnapshotsIndex)
	e.Encode(kv.ClientSeqCache)

	data := Write.Bytes()
	//　这个持久化中其实存了Raft的状态和最新的快照
	kv.PersistSnapshots.SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte){
	if data == nil || len(data) < 1 {	// 快照中也可能没有状态
		return
	}
	Read := bytes.NewBuffer(data)
	d := gob.NewDecoder(Read)

	d.Decode(&kv.KvDictionary)
	d.Decode(&kv.SnapshotsIndex)
	d.Decode(&kv.ClientSeqCache)
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

			//fmt.Printf("数据已经提交 ----------- %d\n", msg.Index)
			if ok { // 这里可能得到两种数据，一个是操作，一个是快照
				if msg.UseSnapshot{
					go func() {
						kv.mu.Lock()
						defer kv.mu.Unlock()
						kv.readSnapshot(msg.Snapshot)
						kv.persisterSnapshot(msg.Index )	// 可以避免在接到快照还没来得及创建的时候又崩了，少一次网络传递
					}()
				}

				if msg.IsSnapshot{	// 对应与commit
					if kv.isUpperThanMaxraftstate(){ // 返回true证明当前应该执行快照
						kv.persisterSnapshot(msg.Index) // 此index以前的数据已经打包成快照了
						//fmt.Printf("进行快照 %d\n",msg.Index)
						kv.rf.CreateSnapshots(msg.Index- 1)	// 填上Raft中返回值的一个坑，就是返回的commitindex+1
					}
				}

				// 一般来说不会出现这种情况，出现了就是bug (PS：后来还是出现了)
				if msg.Command != nil && msg.Index > kv.SnapshotsIndex{
					cmd := msg.Command.(Op)
					//fmt.Printf("recive message: cmd.op %s; key %s ; value %s\n", cmd.Op, cmd.Key, cmd.Value)
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
							//fmt.Printf("client %d; append数据成功 %q：%q \n",cmd.ClientID,cmd.Key,cmd.Value)
						case "Get":
							//fmt.Printf("get 已经提交 %s : %s \n", cmd.Key, cmd.Value)
							kv.ClientSeqCache[cmd.ClientID] = &LatestReply{Seq: cmd.Clientseq, Value: kv.KvDictionary[cmd.Key]}
						default:
							panic("Invalid Operation. Please check all cmd!")
						}
					}
					if Notice, ok := kv.LogIndexNotice[msg.Index - 1]; ok && Notice != nil {
						close(Notice)
						delete(kv.LogIndexNotice, msg.Index - 1)
					}
					kv.mu.Unlock()

				} else {
					//fmt.Printf("msg.Index %d;  kv.SnapshotsIndex %d\n",msg.Index, kv.SnapshotsIndex)
					// 多个客户端并发写入的时候会发生这种事情
					// log.Fatal("ERROR: DealWithapplyCh() receive a lagging snapshots.(Should be intercepted by Raft)")
				}
			} else {
				//fmt.Println("----------------------------------")
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

	// 快照相关
	kv.PersistSnapshots = persister
	kv.SnapshotsIndex = 0
	kv.readSnapshot(kv.PersistSnapshots.ReadSnapshot())

	go kv.DealWithapplyCh()
	return kv
}
