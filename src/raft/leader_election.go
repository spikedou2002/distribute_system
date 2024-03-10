package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300

func (rf *Raft) pastElectionTimeout() bool {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	//DPrintf(222, "%d has refreshed the electionTimeout at term %d to a random value %d...\n", rf.me, rf.currentTerm, rf.electionTimeout/1000000)
}

func (rf *Raft) becomeCandidate() {
	rf.resetElectionTimer()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	//DPrintf(100, "%v :becomes leader and reset TrackedIndex\n", rf.SayMeL())
	rf.resetTrackedIndex()
}
func (rf *Raft) StartElection() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.becomeCandidate()
	term := rf.currentTerm
	done := false
	votes := 1
	args := RequestVoteArgs{}
	args.Term = term
	args.LastLogIndex = rf.getLastEntryTerm()
	args.LastLogIndex = rf.log.LastLogIndex
	defer rf.persist()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok || !reply.VoteGranted {
				//DPrintf(101, "%v: cannot be given a vote by node %v at reply.term=%v\n", rf.SayMeL(), serverId, reply.Term)
				return
			}
			rf.Mu.Lock()
			defer rf.Mu.Unlock()
			if reply.Term < rf.currentTerm {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}
			votes++
			if done || votes < len(rf.peers)/2 {
				return
			}
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			rf.becomeLeader()
			go rf.StartAppendEntries(true)
		}(i)
	}

}
func (rf *Raft) HandleHeartbeatRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resetElectionTimer()
	rf.state = Follower
	if args.LeaderTerm > rf.currentTerm {

	}
}
