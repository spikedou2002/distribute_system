package raft

import (
	"time"
)

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) tryCommitL(matchIndex int) {
	if matchIndex < rf.commitIndex {
		return
	}
	if matchIndex > rf.log.LastLogIndex {
		return
	}
	if matchIndex < rf.log.FirstLogIndex {
		return
	} //out of bound
	if rf.getEntryTerm(matchIndex) != rf.currentTerm {
		return
	}
	cnt := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if matchIndex <= rf.peerTrackers[i].matchIndex {
			cnt++
		}
	}
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex
	} else {

	}
}
