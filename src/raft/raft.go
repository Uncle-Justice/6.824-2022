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
	//	"bytes"
	// "fmt/"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)
const (

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 100
	MinVoteTime  = 75

	// 心跳固定周期，需要比选举超时的下限还要低，这样才能保证心跳机制稳定运行（选举超时指的是，心跳一直等不来，就会自己进入选举状态
	HeartbeatSleep = 35
	AppliedSleep   = 15
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器都要有的持久化的状态
	currentTerm int        //当前周期号，初始为0
	votedFor    int        // 当前周期号把票投给了谁
	logs        []LogEntry // 日志数组，第一个entry的index应从1开始

	// 所有服务器需要的volatile states
	commitIndex int // commit的log entry中最高的index，初始为0
	lastApplied int // 应用进状态机中的最高的index，初始为0

	// leader需要的volatile states
	nextIndex  []int // 对于每一个server，这个leader下一次要发送的log entry的index
	matchIndex []int // 对于每一个server，他们自己的状态机中已经完成复制的log entry的最高index

	applyChan chan ApplyMsg // 用来写入通道，好像主要跟快照有关

	// 论文里没提到但是显然一个server还必须具备的变量
	status Status

	voteNum    int       // 只在server处于candidate状态时生效，记录当前已经收到多少peer投给自己的票
	votedTimer time.Time // 记录本次投票的时间戳
}

// TODO: log index体现在哪里？
type LogEntry struct {
	Term    int
	Command interface{}
}

// 由leader向其他server单向发送
type AppendEntriesArgs struct {
	Term     int // leader当前周期号
	LeaderId int // leader的id
	// PrevLogIndex int // 不太懂

	// PrevLogTerm int        // prevLog的周期号
	Entries []LogEntry // 单次rpc可能会要求存储多个log entry，如果为空，说明本次rpc为心跳

	LeaderCommit int // leader的已经commit的entry的最大index，commit的定义是这个enntry已经被大部分server复制
}

// 由server单向返回给leader
type AppendEntriesReply struct {
	Term int // follower当前的周期号，因为有可能leader也会过时

	Success bool // follower如果匹配了prevLogIndex和prevLogTerm，才能在自己的日志里做追加，如果不匹配会直接返回false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 由candidate单向发送给其他server
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int // candidate的周期号
	CandidateId int // candidate的id，不知道是相对谁的

	// 与快照相关
	// LastLogIndex int // candidate 最后一条entry的index
	// LastLogTerm  int // candidate最后一条entry的周期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool // 是否投票给了该竞选人
}

// server接受REquestVoteRPC
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果rpc的请求比本server的还老，不会给他投票，并且还会回传term，让那个竞选者更新自己的周期号
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm

	// 如果rpc的请求的周期号比本server的新，那么本server不论什么状态，现在都转为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	// 如果日志不conflict的话可以直接把票投给他，因为投票的机制是先到先得

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedTimer = time.Now()
	rf.persist()
	// fmt.Printf("[++++RequestVote++++] :Rf[%v] receive a request vote and agree\n", rf.me)
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// 创建一个Raft对象的指针
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// 需要加锁
	rf.mu.Lock()

	rf.status = Follower

	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{}) // 为什么要先插入一个空的呢

	rf.applyChan = applyCh
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

// 用于非leader的竞选

// follower只要能一直接收到leader或candidate的心跳，
// 就不会变成candidate，follower收到心跳会重置这个election timeout

// 如果follower超过election timeout都没有收到心跳，就会进入竞选，变成candidate状态，
// 自己的【周期号自增】，然后给其他所有server发VoteRPC
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		// 这一次tick设置的随机选举超时时间
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()

		// 如果选举超时发生，即在上面sleep过程中，votedTimer一直没有被更新，本server就要转为candidate，进行选举
		// 如果在上面的sleep过程中，接收到了心跳，那么votedTimer肯定会被更新，则未发生选举超时
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()

			// fmt.Printf("[++++elect++++] :Rf[%v] send a election\n", rf.me)
			rf.sendElection()
			rf.votedTimer = time.Now()
		}
		rf.mu.Unlock()
	}
}

// 用于leader向其他follower【固定周期】传达心跳信号，

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Microsecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// 周期性检查entry的commit情况
func (rf *Raft) committedTicker() {
	for rf.killed() == false {
	}
}

// 向所有peer发送竞选通知，并根据返回的结果，对自己进行一些调整

// 关键在于，发出去到收到回复的过程中，本candidate的状态可能已经发生改动，或者已经跟不上全局的情况了，有可能自己就退出竞选了
func (rf *Raft) sendElection() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 向每个server发送竞选通知的过程，以协程的形式进行，感觉其实不用协程也可以
		// 协程内的return不是结束协程，而是结束协程外的这个函数
		go func(server int) {

			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				// rf.getLastIndex(),
				// rf.getLastTerm(),
			}

			reply := RequestVoteReply{}
			rf.mu.Unlock()

			res := rf.sendRequestVote(server, &args, &reply)

			// res表示对方是否顺利地处理完了这条rpc
			if res == true {
				rf.mu.Lock()

				// 如果自己已经不是candidate或者现在的周期号已经和发竞选通知的周期号不同的时候，之前发出去的竞选rpc可以视作作废
				// 这时再收到之前发出的竞选rpc的回复可以直接无视
				if rf.status != Candidate && args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				// 如果返回的结果显示，已经有其他的server的周期号走在前面了，那么说明自己竞选失败，自己变回follower
				if args.Term < reply.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}

					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted == true && rf.currentTerm == args.Term {
					rf.voteNum += 1

					// 本candidate成为新的leader
					if rf.voteNum >= len(rf.peers)/2+1 {
						// fmt.Printf("[++++elect++++] :Rf[%v] to be leader,term is : %v\n", rf.me, rf.currentTerm)
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0

						rf.persist()
						rf.votedTimer = time.Now()
						rf.mu.Unlock()
						return

					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

// leader向其他server发送AppendEntriesRPC
func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// PrevLogIndex: prevLogIndex,
				// PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			args.Entries = []LogEntry{}

			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			// fmt.Printf("[TIKER-SendHeart-Rf(%v)-To(%v)] args:%+v,curStatus%v\n", rf.me, server, args, rf.status)
			res := rf.sendAppendEntries(server, &args, &reply)

			if res == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status != Leader {
					return
				}

				// 本leader发现有其他server的term已经走在前面，自动放弃leader变为follower
				if reply.Term > rf.currentTerm {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()
						rf.votedTimer = time.Now()
						return
					}
				}
				if reply.Success {
					// commit相关的逻辑
				} else {

				}
			}
		}(index)
	}
}

// server接受AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("[	AppendEntries--Return-Rf(%v) 	] arg:%+v, reply:%+v\n", rf.me, args, reply)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// reply.UpNextIndex = -1
		return
	}
	reply.Success = true
	reply.Term = args.Term
	// reply.UpNextIndex = -1

	// 除非本server的周期号大于AppendEntries的周期号，否则本server不论是什么状态，都变为follower

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	return

}
