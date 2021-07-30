// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"time"
)


// None is a placeholder node ID used when there is no leader.
const None uint64 = 0


// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	//avoid the votes are part, add a random mechanism
	electinorandomInterval int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	//number of ticks since it reached last transfer msg timeout
	transferElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader  transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// Init the raft base, and set the random election time
	rand.Seed(int64(c.ElectionTick))
	r := &Raft{
		id: c.ID,
		Vote: 0,
		RaftLog: newLog(c.Storage),
		State: StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Prs: make(map[uint64]*Progress),
		msgs: make([]pb.Message,0),
		votes: make(map[uint64]bool),
	}
	r.becomeFollower(0, None)

	//read the hardSt and initial config 
	hardState, confState, _ := r.RaftLog.storage.InitialState()
	r.Term, r.Vote, r.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}

	// Init the log
	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			r.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	log.Infof("MsgSendAppend Peer %d, To %d, Term %v, Commit Index %d", r.id, to, r.Term, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
	}

	ents := make([]*pb.Entry,0)
	nextIndex := r.Prs[to].Next
	curTerm,err := r.RaftLog.Term(nextIndex-1)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}
	msg.LogTerm = curTerm

	//apend entry to send
	lastIndex := r.RaftLog.LastIndex();
	for i := nextIndex; i<=lastIndex; i++ {
		ents = append(ents, &r.RaftLog.entries[i-r.RaftLog.firstOffset])
	}

	//update the msg
	msg.Index = nextIndex-1
	msg.Entries = ents
	r.msgs = append(r.msgs, msg)
	return true

}

//send timeout msg
func (r *Raft) sendTimeout(to uint64) {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			Term: r.Term,
			To: to,
			From: r.id,
		}
		r.msgs = append(r.msgs, msg)
		return
}

//send snapshot response
func (r *Raft) sendSnapshotResponse(to uint64, index uint64) {
	log.Infof("snapshot peer %d", r.id)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:r.Term,
		From: r.id,
		To: to,
		Index: index,
	}
	r.msgs = append(r.msgs, msg)
	return
}

//send Snapshot
func (r *Raft) sendSnapshot(to uint64) {
	snap,err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.Prs[to].Next = snap.Metadata.Index + 1
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From: r.id,
		To: to,
		Term: r.Term,
		Snapshot: &snap,
	})
	return
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From: r.id,
		To: to,
		Term: r.Term,
	}
	r.msgs = append(r.msgs, msg)

}

//candidate send vote request
func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From: r.id,
		To: to,
		Term: r.Term,
		Index: r.RaftLog.LastIndex(),
	}
	logTerm,err := r.RaftLog.Term(msg.Index)
	if err != nil {
		panic(err)
	}
	msg.LogTerm = logTerm
	r.msgs = append(r.msgs,msg)
}

//send the response Request Vote
func (r *Raft) sendRequestVoteResponse(to uint64, vote bool) {
	log.Infof("RequesetVoteResponse Peer %d, To %d, vote %v", r.id, to, r.Term)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Reject: !vote,
	}
	r.msgs = append(r.msgs,msg)
}

//send the response of msgappend
func (r *Raft) sendAppendResponse(to uint64, logTerm uint64,lastIndex uint64, reject bool) {
	log.Infof("MsgAppend Peer %d, To %d, Term %v, Reject %v, LogTerm %d, Index %d", r.id, to, r.Term, reject, logTerm, lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		LogTerm: logTerm,
		Index:   lastIndex,
	}
	r.msgs = append(r.msgs, msg)
}


// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateFollower || r.State == StateCandidate {
		r.tickElection()
	} else if r.State == StateLeader {
		r.tickHeartbeat()
	}
}

//control the election of candidate
func (r *Raft)tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout+r.electinorandomInterval {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From: r.id,
			To: r.id,
		})
		r.electionElapsed = 0
	}
}

//control the tick of heartbeat used by leader
func (r *Raft)tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From: r.id,
			To: r.id,
		})
		r.heartbeatElapsed = 0
	}
}



// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// init the Elapsed timer and votes
	log.Infof("Peer id %d become follower", r.id)
	if r.electionTimeout >0 {
		//means set the timeout
		randTemp := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.electinorandomInterval = randTemp.Intn(r.electionTimeout)
	}

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0
	r.State = StateFollower
	r.Vote = None

	//refresh the lead and term
	r.Lead = lead
	r.Term = term

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Infof("Peer id %d become Candidate", r.id)
	if r.electionTimeout >0 {
		//means set the timeout
		randTemp := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.electinorandomInterval = randTemp.Intn(r.electionTimeout)
	}
	r.State = StateCandidate
	//start the election
	r.Term++
	r.Vote=r.id
	r.electionElapsed = 0
	r.transferElapsed = 0

	//refresh the votes record
	r.votes = make(map[uint64]bool)
	//vote for itself
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: r.id,
		Term: r.Term,
		Reject: false,
	})
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).

	log.Infof("Peer id %d become Leader", r.id)
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateLeader {
		//this function cant be called more than once
		r.State = StateLeader
		r.heartbeatElapsed = 0
		r.Lead = r.id
		r.leadTransferee = 0

		//send noop entry
		prelastIndex := r.RaftLog.LastIndex()
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			Term: r.Term,
			Index: prelastIndex+1,
			EntryType: pb.EntryType_EntryNormal,
		})

		//update index
		for peer := range r.Prs {
			if peer == r.id {
				r.Prs[peer]=&Progress{
					Match : prelastIndex+1,
					Next :  prelastIndex+2,
				}
			} else {
				r.Prs[peer]=&Progress{
					Match: 0,
					Next: prelastIndex+1,
				}
			}

		}

		//send noop entry to other nodes
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}

		//if node only has one
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.Prs[r.id].Match
		}
	}



}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	if m.Term > r.Term {
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for id := uint64(1); id <= uint64(len(r.Prs)); id++ {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		case pb.MessageType_MsgTimeoutNow:
			r.handleMsgTimeoutNow(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for id := uint64(1); id <= uint64(len(r.Prs)); id++ {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		case pb.MessageType_MsgTimeoutNow:
			r.handleMsgTimeoutNow(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From)
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for peerId := uint64(1); peerId <= uint64(len(r.Prs)); peerId++ {
				if peerId != r.id {
					r.sendHeartbeat(peerId)
				}
			}
		case pb.MessageType_MsgPropose:
			if r.leadTransferee == None {
				r.handleMsgPropose(m.Entries)
			}
		case pb.MessageType_MsgTimeoutNow:

			r.handleMsgTimeoutNow(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleMsgAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		}
	}
	return nil
}

//handleRequestVoteResponse handle the case candidate receive a Request Vote
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//record the vote
	r.votes[m.From] = !m.Reject

	//get the total of agree and reject vote
	//votes will count until it exists
	voteCount := 0
	rejectCount := 0
	for _, vote := range r.votes {
		if vote {
			voteCount++
		} else {
			rejectCount++
		}
	}

	sumPeer := len(r.Prs)
	if voteCount > sumPeer/2 {
		//get agree half above , the node become leader and send heartbeat immediately
		r.becomeLeader()
		for peerId := uint64(1); peerId <= uint64(len(r.Prs)); peerId++ {
			if peerId != r.id {
				r.sendHeartbeat(peerId)
			}
		}
	} else if rejectCount > sumPeer/2 {
		//if reject half above, the candidate become follower immediately
		r.becomeFollower(r.Term, None)
	}
}

//handleMsgTimeoutNow handle the msg that current leader Timeout Request
func (r *Raft) handleMsgTimeoutNow(m pb.Message) {
	// if the node is new or already is candidate, just skip the msg
	if r.Prs[r.id] != nil || r.State != StateCandidate{
		log.Infof("timeout %d become candidate", r.id)
		r.becomeCandidate()
		for id := uint64(1); id <= uint64(len(r.Prs)); id++ {
			if id != r.id {
				r.sendRequestVote(id)
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From,None,None,true)
		return
	}
	if m.Term > r.Term || (m.Term == r.Term && r.State == StateCandidate){
		r.becomeFollower(m.Term, m.From)
	} else {
		r.Lead = m.From
		r.electionElapsed = 0
	}

	//handle the log

	lastIndex := r.RaftLog.LastIndex()

	if m.Index > lastIndex {
		r.sendAppendResponse(m.From,None, lastIndex+1, true )
		return
	}
	if m.Index >= r.RaftLog.firstOffset {
		lastTerm,err := r.RaftLog.Term(m.Index)
		if err!=nil {
			panic(err)
		}
		if lastTerm != m.LogTerm && m.LogTerm != 0{
			idx := int(m.Index - r.RaftLog.firstOffset+1)
			i := sort.Search(idx, func(i int) bool { return r.RaftLog.entries[i].Term == lastTerm })
			lastIndex := uint64(uint64(i) + r.RaftLog.firstOffset)
			r.sendAppendResponse(m.From,lastTerm,lastIndex, true)
			return
		}
	}

	for i, entry := range m.Entries {
		if entry.Index < r.RaftLog.firstOffset {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				idx :=int(entry.Index - r.RaftLog.firstOffset)
				r.RaftLog.entries[idx] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, None, r.RaftLog.LastIndex(),false)


}

//handleTransferLeader handle the leadership transfering by current leader
func (r *Raft) handleTransferLeader(m pb.Message) {
	if r.State != StateLeader {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		return
	}
	peer,ok := r.Prs[m.From]
	if !ok {
		return
	} else if( m.From == r.id) {
		return
	}
	r.leadTransferee  = m.From
	r.transferElapsed = 0
	if peer.Match == r.RaftLog.LastIndex() {
		r.sendTimeout(m.From)
		return
	}
	r.sendAppend(m.From)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To:m.From,
		Term: r.Term,
		Reject: false,
	}
	if m.Term != None && m.Term < r.Term {
		msg.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleMsgPropose(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		//TODO confirm the lastIndex +1
		entry.Index = lastIndex + uint64(i) +1
		log.Infof("MsgPropose id:%v enrty:term index %v %v self index: %v\n",r.id, entry.Term,entry.Index,r.RaftLog.committed)
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = entry.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	log.Infof("MsgAppendResponse Term %d, Index %d logTerm %d commit %d",m.Term,m.Index,m.LogTerm,m.Commit)
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index
		if index == None {
			return
		}
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIndex := sort.Search(len(l.entries),
				func(i int) bool { return l.entries[i].Term > logTerm })
			if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
				index = uint64(sliceIndex) + l.firstOffset
			}
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		match := make(uint64Slice, len(r.Prs))
		i := 0
		for _, prs := range r.Prs {
			match[i] = prs.Match
			i++
		}
		sort.Sort(match)
		n := match[(len(r.Prs)-1)/2]

		if n > r.RaftLog.committed {
			logTerm, err := r.RaftLog.Term(n)
			if err != nil {
				panic(err)
			}
			if logTerm == r.Term {
				r.RaftLog.committed = n
				for peer := range r.Prs {
					if peer == r.id {
						continue
					}
					r.sendAppend(peer)
				}
			}
		}
		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				From:    r.id,
				To:      m.From,
			}
			r.msgs = append(r.msgs, msg)
			r.leadTransferee = None
		}
	}
}

//handleRequestVote handle RequestVote request
func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term!=None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, false)
		return

	}
	/*
	if r.Vote != None && r.Vote != m.From && r.State == StateFollower {
		r.sendRequestVoteResponse(m.From, false)
		return
	}
	 */
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if r.RaftLog.stabled == 0 && m.LogTerm ==0 && r.Vote != None && r.Vote != m.From{
		// the log condition is not  considered
		r.sendRequestVoteResponse(m.From, false)
		return
	}
	if  ((r.Vote==None && r.Lead == 0) || (r.Vote == m.From)) && (((lastTerm<m.LogTerm) ||
		(lastTerm == m.LogTerm && lastIndex<= m.Index)) ||
		r.RaftLog.entries == nil) {
		//r.becomeFollower(m.Term, None)
		r.electionElapsed = 0
		r.Vote = m.From
		r.votes[r.id] = false
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.sendRequestVoteResponse(m.From, false)


}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Snapshot == nil {
		//snap is empty
		//fmt.Println("Snapshot empty")
		return
	}
	metaData := m.Snapshot.Metadata
	if r.RaftLog.committed >= metaData.Index {
		r.sendSnapshotResponse(m.From, r.RaftLog.committed)
		return
	}
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range metaData.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.RaftLog.firstOffset = metaData.Index + 1
	//fmt.Printf("\nfo %v\n", r.RaftLog.firstOffset)
	r.RaftLog.committed = metaData.Index
	r.RaftLog.entries = nil
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.stabled = metaData.Index
	r.RaftLog.applied = metaData.Index
	r.sendSnapshotResponse(m.From, r.RaftLog.LastIndex())
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			match := make(uint64Slice, len(r.Prs))
			i := 0
			for _, prs := range r.Prs {
				match[i] = prs.Match
				i++
			}
			sort.Sort(match)
			n := match[(len(r.Prs)-1)/2]

			if n > r.RaftLog.committed {
				logTerm, err := r.RaftLog.Term(n)
				if err != nil {
					panic(err)
				}
				if logTerm == r.Term {
					r.RaftLog.committed = n
					r.bcastAppend()
				}
			}
		}
	}
	r.PendingConfIndex = None
}

//get the proper follower if current leader need to be removed
func (r *Raft) GetProperFollower() uint64 {
	if r.State != StateLeader {
		return 0
	}
	if len(r.Prs) <= 1 {
		return 0
	}
	maxLogIdx := r.RaftLog.LastIndex()
	var biggestId uint64
	var biggestLog uint64
	for id, pr := range r.Prs {
		if id == r.id {
			continue
		}
		if pr.Match == maxLogIdx {
			return id
		}
		if pr.Match > biggestLog {
			biggestLog = pr.Match
			biggestId = id
		}
	}
	return biggestId
}
