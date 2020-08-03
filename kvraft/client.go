package raftkv

import (
	"MapReduce/6.824/src/labrpc"
	//	"fmt"

	"math/big"
	"crypto/rand"
	mrand "math/rand"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int   // 记录哪一个是leader
	ClientID int64 // 记录当前客户端的序号
	seq      int64 // 当前的操作数
}

var mp = make(map[int64]bool)
var mu sync.Mutex

func nrand() int64 { // 如果直接用这个获取全局唯一ID的话可能会使得Client重复
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 放到真正的分布式环境中的话应该使用一个可以获取全局唯一ID的一个方法
func Deduplication() int64 {
	mu.Lock()
	for {
		temp := nrand()
		if mp[temp] {
			continue
		}
		mp[temp] = true
		mu.Unlock()
		return temp
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = Deduplication()
	ck.seq = 1
	ck.leader = mrand.Intn(len(servers)) // 随机选择一个起始值 生成(0,len(server)-1)的随机数
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 默认一次执行一个 所以不需要加锁
func (ck *Clerk) Get(key string) string {
	//fmt.Printf("%d Clerk: Get: %q, seq is %d\n", ck.ClientID, key, ck.seq)
	// You will have to modify this function.
	serverLength := len(ck.servers)
	for {
		args := &GetArgs{key, ck.ClientID, ck.seq}
		reply := new(GetReply)

		ck.leader %= serverLength //得到此次通信的leader
		replyArrival := make(chan bool)
		go func() {
			ok := ck.servers[ck.leader].Call("RaftKV.Get", args, reply)
			replyArrival <- ok
		}()
		select {
		case ok := <-replyArrival:
			if ok && !reply.WrongLeader {
				if reply.Err == OK || reply.Err == Duplicate || reply.Err == ErrNoKey {
					ck.seq++
					//fmt.Println("得到数据 : " + reply.Value)
					return reply.Value
				} else { // ReElection 我们需要重新发送请求 即重新选主
					ck.leader++
				}
			} else if reply.Err == NoLeader {
				ck.leader++
			}
		case <- time.After(3 * time.Second): // 超时以后当然也要重新发送了
			ck.leader++
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//fmt.Printf("Clerk: PutAppend: %q => (%q,%q) from: %d; seq is %d\n", op, key, value, ck.ClientID, ck.seq)
	serverLength := len(ck.servers)
	for {
		args := &PutAppendArgs{key, value, op, ck.ClientID, ck.seq}
		reply := new(PutAppendReply)
		ck.leader %= serverLength
		replyArrival := make(chan bool)
		go func() {
			ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, reply)
			replyArrival <- ok
		}()
		select {
		case <-time.After(3 * time.Second):
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == Duplicate) {
				ck.seq++
				return
			} // 重新发送请求就是返回值为ReElection
			ck.leader++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
