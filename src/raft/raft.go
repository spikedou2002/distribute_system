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

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	dead             int32               // set by Kill()
	peerTrackers     []PeerTracker
	state            int
	currentTerm      int
	votedFor         int
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration //选举计时器
	//peerTrackers     []PeerTracker
	log           *Log
	lastElection  time.Time // 上一次的选举时间，用于配合since方法计算当前的选举时间是否超时
	lastHeartbeat time.Time // 上一次的心跳时间，用于配合since方法计算当前的心跳时间是否超时
	heart         chan bool
	//volatile state
	commitIndex int
	lastApplied int
	//for leader only
	nextIndex  []int
	matchIndex []int
	//2B
	applyCh chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	snapshot                 []byte
	snapshotLastIncludeIndex int
	snapshotLastIncludeTerm  int
}

const Leader = 1
const Candidate = 2
const Follower = 3
const tickInterval = 100 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	FollowerTerm int
	Success      bool
	PrevLogIndex int
	PrevLogTerm  int
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
func (rf *Raft) sendRequestAppendEntries(isHeartbeat bool, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ok bool
	if isHeartbeat {
		ok = rf.peers[server].Call("Raft.HandleHeartbeatRPC", args, reply)
	} else {
		ok = rf.peers[server].Call("Raft.HandleAppendEntriesRPC", args, reply)
	}
	return ok
}
func (rf *Raft) StartAppendEntries(heart bool) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.resetElectionTimer()
	if rf.state != Leader {
		return
	}
	// 并行向其他节点发送心跳或者日志，让他们知道此刻已经有一个leader产生
	//DPrintf(111, "%v: detect the len of peers: %d", rf.SayMeL(), len(rf.peers))
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart)

	}
}
func (rf *Raft) AppendEntries(targetServerId int, heart bool) {
	if heart {
		reply := AppendEntriesReply{}
		args := AppendEntriesArgs{}
		rf.Mu.Lock()
		if rf.state != Leader {
			return
		}
		args.LeaderTerm = rf.currentTerm
		rf.Mu.Unlock()
		ok := rf.sendRequestAppendEntries(true, targetServerId, &args, &reply)
		rf.Mu.Lock()
		if !ok {
			rf.Mu.Unlock()
			return
		}
		if rf.state != Leader {
			rf.Mu.Unlock()
			return
		}
		if reply.FollowerTerm < rf.currentTerm {
			rf.Mu.Unlock()
			return
		}
		if reply.FollowerTerm > rf.currentTerm {
			rf.currentTerm = reply.FollowerTerm
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
			rf.Mu.Unlock()
			return
		}
		if reply.Success {
			rf.Mu.Unlock()
			return
		}
		rf.Mu.Unlock()
		return
	} else {
		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		rf.Mu.Lock()
		if rf.state != Leader {
			rf.Mu.Unlock()
			return
		}
		if rf.log.LastLogIndex < rf.peerTrackers[targetServerId].nextIndex-1 {
			args.PrevLogIndex = rf.log.LastLogIndex
		} else {
			args.PrevLogIndex = rf.peerTrackers[targetServerId].nextIndex - 1
		}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogTerm = rf.getEntryTerm(args.PrevLogIndex)
		args.Entries = rf.log.getAppendEntries(args.PrevLogIndex + 1)
		rf.Mu.Unlock()
		ok := rf.sendRequestAppendEntries(false, targetServerId, &args, &reply)
		if !ok {
			//DPrintf(111, "%v: cannot request AppendEntries to %v args.term=%v\n", rf.SayMeL(), targetServerId, args.LeaderTerm)
			return
		}

		rf.Mu.Lock()
		defer rf.Mu.Unlock()
		if rf.state != Leader {
			return
		}
		if reply.FollowerTerm < rf.currentTerm {
			return //an outdated reply, maybe due to internet failure, drop this reply
		}
		if reply.FollowerTerm > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.FollowerTerm
			rf.persist()
			return
		}
		if reply.Success {
			rf.peerTrackers[targetServerId].nextIndex = args.PrevLogIndex + len(args.Entries) + 1
			rf.peerTrackers[targetServerId].matchIndex = args.PrevLogIndex + len(args.Entries)
			rf.tryCommitL(rf.peerTrackers[targetServerId].matchIndex)
			return
		}
		//reply failed
		if reply.PrevLogIndex > rf.log.LastLogIndex {
			rf.peerTrackers[targetServerId].nextIndex = rf.log.LastLogIndex + 1
		} else if rf.getEntryTerm(reply.PrevLogIndex) == reply.PrevLogTerm {
			rf.peerTrackers[targetServerId].nextIndex = reply.PrevLogIndex + 1
		} else {
			PrevIndex := reply.PrevLogIndex
			for PrevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(PrevIndex) == rf.getEntryTerm(reply.PrevLogIndex) {
				PrevIndex--
			}
			rf.peerTrackers[targetServerId].nextIndex = PrevIndex + 1
		}
	}
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
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.log.LastLogIndex + 1
	// 开始发送AppendEntries rpc
	rf.log.appendL(Entry{term, command})
	rf.persist()
	//rf.resetTrackedIndex()
	go rf.StartAppendEntries(false)

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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.Mu.Lock()
		state := rf.state
		rf.Mu.Unlock()
		switch state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() {
				rf.StartElection()
			}
		case Leader:
			// 只有Leader节点才能发送心跳和日志给从节点
			isHeartbeat := false
			// 检测是需要发送单纯的心跳还是发送日志
			// 心跳定时器过期则发送心跳，否则发送日志
			if rf.pastHeartbeatTimeout() {
				isHeartbeat = true
				rf.resetHeartbeatTimer()
			}
			rf.StartAppendEntries(isHeartbeat)
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			//ms := 50 + (rand.Int63() % 300)
			//time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		time.Sleep(tickInterval)
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
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.log = NewLog()
	rf.resetElectionTimer()
	rf.heartbeatTimeout = heartbeatTimeout
	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.peerTrackers = make([]PeerTracker, len(rf.peers)) //对等节点追踪器
	rf.applyCh = applyCh
	//initialize for snap shot
	rf.snapshot = nil
	rf.snapshotLastIncludeIndex = 0
	rf.snapshotLastIncludeTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
func (rf *Raft) getLastEntryTerm() int {
	if rf.log.LastLogIndex >= rf.log.FirstLogIndex {
		return rf.log.getOneEntry(rf.log.LastLogIndex).Term
	} else {
		return rf.snapshotLastIncludeTerm
	}
	return -1

}
func (rf *Raft) GetLastIncludeIndex() int {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.snapshotLastIncludeIndex
}
func (rf *Raft) GetSnapshot() []byte {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.snapshot
}
