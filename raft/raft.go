package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"MapReduce/6.824/src/labrpc"
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"time"
)

//import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// 不清楚上面的有什么用

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int        // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int        // 在当前获得选票的候选人的 Id
	logs        []LogEntry // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	// Volatile state on all servers
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	// Volatile state on leaders
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值
	// 以上成员来源于论文

	votes_counts     int           // 记录此次投票中获取的票数 2A
	current_state    string        // 记录当前是三个状态里的哪一个 2A
	timer            *time.Timer   // 对于每一个raft对象都需要一个时钟 在超时是改变状态 进行下一轮的选举 2A
	electionTimeout  time.Duration // 150-300ms 选举的间隔时间不同 可以有效的防止选举失败 2A
	heartbeatTimeout time.Duration // 心跳超时 论文中没有规定时间 但要小于选举超时 我选择50-100ms

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { // 2A

	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	isleader = rf.current_state == "LEADER"

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //候选人的任期号 2A
	CandidateId  int // 请求选票的候选人ID 2A
	LastLogIndex int // 候选人的最后日志条目的索引值 2A
	LastLogTerm  int // 候选人的最后日志条目的任期号 2A
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // 当前任期号,便于返回后更新自己的任期号 2A
	VoteGranted bool // 候选人赢得了此张选票时为真 2A
}

func (rf *Raft) handleTimer() { // 超时事件 // 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.current_state != "LEADER" { // 超时时转换成候选者进行选举
		rf.current_state = "CANDIDATE" // 改变当前状态为candidate
		rf.currentTerm += 1            // Term加1
		rf.votedFor = rf.me            // 票投给自己handleTimer
		rf.votes_counts = 1            // 目前有一票

		rf.persist() // 2C

		args := RequestVoteArgs{
			Term:         rf.currentTerm,   // 请求者的纪元
			CandidateId:  rf.me,            // 请求者的ID
			LastLogIndex: len(rf.logs) - 1, // 这一项用于在选举中选出最数据最新的节点 论文[5.4.1]
		}
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}

		for serverNumber := 0; serverNumber < len(rf.peers); serverNumber++ { // 和其他服务器通信 请求投票给自己
			if serverNumber == rf.me { // 当然不需要和自己通信啦
				continue
			}
			go func(server int, args RequestVoteArgs, rf *Raft) { // 并行效率更高
				var reply RequestVoteReply
				//retry:

				ok := rf.sendRequestVote(server, args, &reply) //进行RPC
				if ok {
					rf.handleVoteResult(reply) //对于获取到的结果进行处理
				} else {
					//fmt.Printf("对 %d 调用RPC失败\n",server)
					rf.resetTimer() // 很重要 否则在出现follower网络分区的时候这个节点Term会一直暴涨
					//rf.stopTimer()
					//got oretry
				}
			}(serverNumber, args, rf)
		}
	} else {

	}
	rf.resetTimer() // 重置超时事件
}

func (rf *Raft) resetTimer() {// 2A 2B
	if rf.timer != nil {
		rf.timer.Stop()
	}
	switch rf.current_state { // 因为根据节点状态的不同 超时时间和事件并不相同
	case "LEADER":
		rf.timer = time.AfterFunc(rf.heartbeatTimeout, func() { rf.SendAppendEntriesToAllFollwer() })
	case "CANDIDATE":
		rf.timer = time.AfterFunc(rf.electionTimeout, func() { rf.handleTimer() })
	case "FOLLOWER":
		rf.timer = time.AfterFunc(rf.electionTimeout, func() { rf.handleTimer() })
	default:
		log.Fatal("Error in Raft.resettimer, undefined behaviour.")
	}
}

func (rf *Raft) stopTimer() {
	if rf.timer != nil {
		rf.timer.Stop()
	}
}

/*
 * 当投票结果返回的时候执行处理的函数
 * 这里可能有三种情况
 * 1.返回值Trem小于发送者 -> 无效 抛弃这条信息
 * 2.返回值Trem大于发送者 -> 状态转移为follower 并更新纪元
 * 3.投票有效
 */
func (rf *Raft) handleVoteResult(reply RequestVoteReply) { // 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 收到的纪元小于当前纪元
	if reply.Term < rf.currentTerm {
		return
	}

	// 收到的纪元大于当前纪元
	if reply.Term > rf.currentTerm {
		rf.current_state = "FOLLOWER"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	// 条件满足的话此次投票有效 否则的话就不用管了
	if rf.current_state == "CANDIDATE" && reply.VoteGranted {
		rf.votes_counts += 1                      // 投票+1
		if rf.votes_counts >= len(rf.peers)/2+1 { //票数超过全部节点的一半
			rf.current_state = "LEADER"
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.votedFor = -1
				rf.nextIndex[i] = len(rf.logs) // 把日志更新到和leader一样
				rf.matchIndex[i] = -1          //这里还不清楚是干什么的
			}
			rf.SendAppendEntriesToAllFollwer() // 发送心跳包 确定leader地位
			//fmt.Printf("重新选举成功 %d 成为leader Term 为 %d \n",rf.me,rf.currentTerm)
			rf.resetTimer()                    // 重置超时事件
		}
		return
	}
}

//
// example RequestVote RPC handler.
// 也就是收到投票以后干什么
/*
 * 我们在这个函数中需要实现将请求者的日志和被请求者的日志作对比
 * 1.比较最后一项日志的Term，也就是LastLogTerm，相同的话比较索引，也就是LastLogIndex，如果当前节点较新的话就不会投票
 * 2.如果当前节点的Term比候选者节点的Term大，拒绝投票
 * 3.如果当前节点的Term比候选者节点的Term小，那么当前节点转换为Follwer状态
 * 4.判断是否已经投过票，如果没投票并且能投票(日志更旧)，那么就投票给该候选者
 */
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) { // 2A
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voting := true

	// 此节点日志比请求者的日志新
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm &&
				len(rf.logs)-1 > args.LastLogIndex) {
			voting = false
		}
	}

	// 此节点Trem大于请求者 忽略请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 此节点Trem小于请求者
	if args.Term > rf.currentTerm {
		rf.current_state = "FOLLOWER"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votes_counts = -1

		if voting {
			rf.votedFor = args.CandidateId
		}
		rf.persist() // 2C

		rf.resetTimer() //重置超时事件
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

	// Term相同 判断是否需要投票
	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && voting { // 未投过票且日志没有问题 可以投票
			rf.votedFor = args.CandidateId

			rf.persist() // 2C
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.current_state != "LEADER"{
		return index, term ,isLeader
	}

	nlog := LogEntry{command, rf.currentTerm}
	isLeader = (rf.current_state == "LEADER")
	rf.logs = append(rf.logs, nlog) // 提交一个命令其实就是向日志里面添加一项 在心跳包的时候同步

	//fmt.Printf("leader append log [leader=%d], [term=%d], [command=%v]\n",
	//rf.me, rf.currentTerm, command)

	index = len(rf.logs)
	term = rf.currentTerm

	rf.persist() // 2C

	return index, term, isLeader
}

// append log 的args和reply
type AppendEntryArgs struct { // 2B
	Term         int        // leader的任期号
	LeaderId     int        // leaderID 便于进行重定向
	PrevLogIndex int        // 新日志之前日志的索引值
	PrevLogTerm  int        // 新日志之前日志的Term
	Entries      []LogEntry // 存储的日志条目 为空时是心跳包
	LeaderCommit int        // leader已经提交的日志的索引
}

type AppendEntryReply struct { // 2B
	Term        int  // 用于更新leader本身 因为leader可能会出现分区
	Success     bool // follower如果跟上了PrevLogIndex,PrevLogTerm的话为true,否则的话需要与leader同步日志
	CommitIndex int  // 用于返回与leader.Term的匹配项,方便同步日志
}

// 2B
func (rf *Raft) SendAppendEntryToFollower(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 用于发送附加日志项给其他服务器 也就是心跳包 超时时间为heartbeatTimeout // 2B
func (rf *Raft) SendAppendEntriesToAllFollwer() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntryArgs

		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1 // 当前最新的索引

		if args.PrevLogIndex >= 0 {
			//fmt.Printf("%d %d\n",args.PrevLogIndex,len(rf.logs))
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		// 当我们在Start中加入一条新日志的时候这里会在心跳包中发送出去
		if rf.nextIndex[i] < len(rf.logs) { // 刚成为leader的时候更新过 所以第一次entry为空
			args.Entries = rf.logs[rf.nextIndex[i]:] //如果日志小于leader的日志的话直接拷贝日志
		}
		args.LeaderCommit = rf.commitIndex

		go func(servernumber int, args AppendEntryArgs, rf *Raft) {
			var reply AppendEntryReply

		retry :

			if rf.current_state != "LEADER"{
				return
			}
			ok := rf.SendAppendEntryToFollower(servernumber, args, &reply)
			if ok {
				rf.handleAppendEntries(servernumber, reply)
			}else {
				goto retry //附加日志失败的时候重新附加
			}
		}(i, args, rf)
	}
}

// follower节点收到appendentries以后的处理
/*
 * 1.判断当前Term和leaderTerm的大小 前者大于后者的话拒绝 小于的话改变节点状态
 * 2.进行一个错误判断,Leader节点保存的nextIndex为leader节点日志的总长度，而Follwer节点的日志数目可能不大于nextIndex,
     原因是可能这个follower原来可能是leader,一部分数据还没有提交,或者原来就是follower,但是有一些数据丢失,此时要使leader
     减少PrevLogIndex来寻找来年各个节点相同的日志。论文[5.3]
 * 3.Follwer节点的日志数目比Leader节点记录的NextIndex多，则说明存在冲突，则保留PrevLogIndex前面的日志,在尾部添加RPC请求
	 中的日志项并提交日志
 * 4.如果RPC请求中的日志项为空，则说明该RPC请求为Heartbeat，改变当前节点状态,因为可能此节点当前还是CANDIDATE,并提交未提交的日志
*/
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {// 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		// 没必要重置时钟 因为出现一个节点收到落后于自己的Term我认为只可能在分区的时候,这个时候的这个leader其实没有什么意义
		rf.resetTimer()
		return
	} else {
		//fmt.Printf("%d 在Term %d 中接到消息附加日志或者心跳包 leader Term为%d\n",rf.me,rf.currentTerm, args.Term)
		rf.current_state = "FOLLOWER" // 改变当前状态
		rf.currentTerm = args.Term    // 落后于leader的时候更新Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm

		if args.PrevLogIndex >= 0 && // 首先leader有日志
			(len(rf.logs)-1 < args.PrevLogIndex || // 此节点日志小于leader 也就是说下一行数组不会越界 即日志一定大于等于PrevLogIndex
				rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) { // 或者在相同index上日志不同
			reply.CommitIndex = len(rf.logs) - 1
			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex //多出的日志一定会被舍弃掉 要和leader同步
			}
			for reply.CommitIndex >= 0 {
				if rf.logs[reply.CommitIndex].Term != args.Term {
					reply.CommitIndex--
				} else {
					break
				}
			}
			//返回false说明要此节点日志并没有更上leader,或者有多余或者不一样的地方
			//出现的原因是这个节点以前可能是leader,在一些日志并没有提交之前就宕机了
			reply.Success = false
		} else if args.Entries == nil { // 心跳包 用于更新状态
			if rf.lastApplied+1 <= args.LeaderCommit { //TODO len(rf.logs)-1 改为 rf.lastApplied+1
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogs() // 可能提交的日志落后与leader 同步一下日志
			}
			reply.CommitIndex = len(rf.logs) - 1
			reply.Success = true
		} else { //日志项不为空 与leader同步日志
			rf.logs = rf.logs[:args.PrevLogIndex+1] // debug: 第一次调用PrevLogIndex为-1
			rf.logs = append(rf.logs, args.Entries...)

			if rf.lastApplied+1 <= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit // 与leader同步信息
				go rf.commitLogs()
			}

			reply.CommitIndex = len(rf.logs) - 1
			if args.LeaderCommit > rf.commitIndex{
				if(args.LeaderCommit < len(rf.logs) - 1){
					reply.CommitIndex = args.LeaderCommit
				}
			}
			reply.Success = true
		}
		//TODO fmt.Printf("rf.me %d , Term %d, 是否成功 %t\n",rf.me,rf.currentTerm, reply.Success)
		rf.persist() // 2C
		rf.resetTimer()
	}
}

// 提交日志
func (rf *Raft) commitLogs() { // 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.logs)-1 {
		log.Fatal("出现错误 : raft.go commitlogs()")
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ { //commit日志到与Leader相同
		// listen to messages from Raft indicating newly committed messages.
		// 调用过程才test_test.go -> start1函数中
		//TODO 这里为什么加1
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

// leader发送附加日志得到回复以后的处理函数
/*
 * 1.如果返回值中的Term大于leader的Term,证明出现了分区,节点状态转换为follower
 * 2.如果RPC成功的话更新leader对于各个服务器的状态
 * 3.如果RPC失败的话证明两边日志不一样,使用前面提到的reply.CommitIndex作为nextIndex,用于请求参数中的PrevLogIndex
 */
// nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
// matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值
func (rf *Raft) handleAppendEntries(server int, reply AppendEntryReply) {// 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.current_state != "LEADER" {
		//log.Fatal("Error in handleAppendEntries, receive a heartbeat reply, but not a leader.")
		return
	}

	if reply.Term > rf.currentTerm { // 出现网络分区 这是一个落后的leader
		rf.current_state = "FOLLOWER"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.CommitIndex + 1 //CommitIndex为对端确定两边相同的index 加上1就是下一个需要发送的日志
		rf.matchIndex[server] = reply.CommitIndex
		//rf.nextIndex[server] = int(math.Min(float64(reply.CommitIndex + 1),float64(len(rf.logs)-1)))
		if rf.nextIndex[server] > len(rf.logs){ //debug
			rf.nextIndex[server] = len(rf.logs)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			//fmt.Printf("2ERROR : %d %d \n",rf.nextIndex[server] , len(rf.logs))
			//log.Fatal("ERROR : rf.nextIndex[server] > len(rf.logs)\n")
		}


		commit_count := 1

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			// 这里可以和其他服务器比较matchIndex 当到大多数的时候就可以提交这个值
			if rf.matchIndex[i] >= rf.matchIndex[server] { //matchIndex 对于每一个服务器，已经复制给他的日志的最高索引值
				commit_count++
			}
		}
		// TODO fmt.Printf("%d 在Term %d 中, 有 %d 个节点同意\n",rf.me, rf.currentTerm, commit_count)

		//fmt.Printf("rf.matchIndex[server] %d %d\n",rf.matchIndex[server],len(rf.logs))
		if commit_count >= len(rf.peers)/2+1 &&
			rf.commitIndex < rf.matchIndex[server] && //保证幂等性 即同一条日志正常只会commit一次
			rf.logs[rf.matchIndex[server]].Term == rf.currentTerm{
			//fmt.Printf("在Term : %d 中, index : %d 的日志已经提交\n", rf.currentTerm, server)
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs() //提交日志 下次心跳的时候会提交follower中的日志
		}
	} else {
		//rf.nextIndex[server] = int(math.Min(float64(reply.CommitIndex + 1),float64(len(rf.logs)-1)))
		rf.nextIndex[server] = reply.CommitIndex + 1

		if rf.nextIndex[server] > len(rf.logs){ //debug
			rf.nextIndex[server] = len(rf.logs)
			//fmt.Printf("1ERROR : %d %d \n",rf.nextIndex[server] , len(rf.logs))
			//log.Fatal("ERROR : rf.nextIndex[server] > len(rf.logs)\n")
		}
		rf.SendAppendEntriesToAllFollwer() //TODO 发送心跳包 其实发送单个人即可 有问题后面再改
	}
	rf.resetTimer() // TODO 很重要 要不后面不发心跳包 导致不停的选举 Term往上飙
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft { // 2A
	/*
		peers参数: 是通往其他Raft端点处于连接状态下的RPC连接
		me参数: 是自己在端点数组中的索引
	*/
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))  // 记录 ”每一个“ 服务器需要发送的下一个日志索引值
	rf.matchIndex = make([]int, len(peers)) //

	rf.current_state = "FOLLOWER" //初始状态为follower
	rf.applyCh = applyCh
	rf.electionTimeout = time.Millisecond * time.Duration(150+rand.Intn(150))
	rf.heartbeatTimeout = time.Millisecond * time.Duration(50+rand.Intn(50))

	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist() // 2C

	return rf
}
