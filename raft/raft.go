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
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
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
	StateFollower  StateType = iota
	StateCandidate
	StateLeader
)

const (
	SnapshotNoNeed StateType = iota
	SnapshotWaiting
	SnapshotSending
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

	// if leader need to send snapshot to one node, and snapshot is not ready or snapshot has been sent.
	// use a flag to state
	snapState StateType

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


	// If the network partition the raft group
	// use the isolated to record the isolated node for the leader
	isolates map[uint64]bool

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	//avoid the votes are part, add a random mechanism
	electionRandomInterval int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int


	// To avoid generating snapshot too frequently
	// use a snapshotElapsed to show if the snapshot is ready
	snapshotElapsed int

	pendingSnapshotIndex uint64

	// If the snapshotElapsed timeout, means snapshot is ready, could try to send again
	snapReady bool

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

	// If transferee's log is not up to date, leader will help the transferee
	// Use a flag to figure out whether transferee need help
	helpTransferee bool


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
		isolates: make(map[uint64]bool),
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

	// log.Infof("MsgSendAppend Peer %d, To %d, Term %v, Commit Index %d", r.id, to, r.Term, r.RaftLog.committed)
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

	if err != nil || nextIndex < r.RaftLog.firstOffset{
		log.Infof("Leader %d send append to %d, Errcompacted, next index %d.", r.id, to, nextIndex )
		r.sendSnapshot(to)
		return false
	}
	msg.LogTerm = curTerm

	//append entry to send
	lastIndex := r.RaftLog.LastIndex()
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
	if r.Prs[to].snapState == SnapshotWaiting && r.snapReady == false {
		log.Infof("Peer %d Snapshot not ready.",r.id)
		return
	}
	if r.Prs[to].snapState == SnapshotSending {
		log.Infof("Peer %d Snapshot is sending",r.id)
		return
	}
	snap,err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Infof("Snapshot not available.")
		r.Prs[to].snapState = SnapshotWaiting
		r.snapReady = false
		r.snapshotElapsed = 0
		r.pendingSnapshotIndex = 0
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
	// snapshot is finished
	r.pendingSnapshotIndex = snap.Metadata.Index
	r.Prs[to].snapState = SnapshotSending
	return
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	// Your Code Here (2A).
	if  r.checkIsolate(){
		log.Infof("Peer %d isolated, become candidate", r.id)
		r.becomeFollower(r.id, None)
		return
	}
	msgs := make([]pb.Message,0)
	for peer := range r.Prs {
		if peer != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
			}
			msgs = append(msgs, msg)
			r.isolates[peer] = true
		}
	}

	r.msgs = append(r.msgs, msgs...)
}

//candidate send vote request
func (r *Raft) sendRequestVote(to uint64) {
	log.Infof("peer %d send vote request to %d", r.id, to)
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
		r.tickSnapShot()
		if r.leadTransferee != None {
			r.tickTransferLeader()
		}
	}

}

//
func (r *Raft) tickSnapShot() {
	r.snapshotElapsed++
	if r.snapshotElapsed > r.heartbeatTimeout {
		r.snapReady = true
		r.snapshotElapsed = 0
		for peer := range r.Prs {
			if peer != r.id {
				if r.Prs[peer].snapState == SnapshotWaiting {
					r.sendSnapshot(peer)
				}
			}
		}
	}

}

// tickTransferLeader advances the
func (r *Raft) tickTransferLeader() {
	r.transferElapsed++
	if r.transferElapsed > r.electionTimeout+r.electionRandomInterval {
		log.Infof("Transfer %d to %d timeout, failed.", r.id, r.leadTransferee)
		r.helpTransferee = false
		r.transferElapsed = 0
		r.leadTransferee = None
	}
}

//control the election of candidate
func (r *Raft)tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout+r.electionRandomInterval {
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
		r.electionRandomInterval = randTemp.Intn(r.electionTimeout)
	}


	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.helpTransferee = false
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
		r.electionRandomInterval = randTemp.Intn(r.electionTimeout)
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

	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateLeader {
		//this function cant be called more than once
		r.State = StateLeader
		r.heartbeatElapsed = 0
		r.Lead = r.id
		r.pendingSnapshotIndex = 0
		r.leadTransferee = None
		r.transferElapsed = 0
		r.helpTransferee = false
		r.snapReady = false
		r.snapshotElapsed = 0

		// propose a noop entry
		preLastIndex := r.RaftLog.LastIndex()
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			Term:      r.Term,
			Index:     preLastIndex +1,
			EntryType: pb.EntryType_EntryNormal,
		})

		// initialize the Match and Next index for all nodes,
		// and initialize the isolates too.
		for peer := range r.Prs {
			r.isolates[peer] = false
			if peer == r.id {
				r.Prs[peer]=&Progress{
					Match : preLastIndex +1,
					Next :  preLastIndex +2,
					snapState: SnapshotNoNeed,
				}
			} else {
				r.Prs[peer]=&Progress{
					Match: 0,
					Next:  preLastIndex +1,
					snapState: SnapshotNoNeed,
				}
			}

		}

		// send noop entry to other nodes
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}

		// if node only has one, it could commit the noop entry directly
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.Prs[r.id].Match
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Infof("Peer %d step %v from %d to %d" , r.id, m.MsgType,m.From,m.To)

	// In any case, if the node receive a message which term is bigger than itself,
	// it become follower directly.
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendRequestVote(peer)
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
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	}
	return nil
}

// stepLeader step the message as a leader state
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.sendHeartbeat()
	case pb.MessageType_MsgPropose:
		// if here transfer leader to a node, the leader should stop other proposals
		if r.leadTransferee != None {
			log.Infof("Leader %d is transfering, drop the propose.", r.id)
			return ErrProposalDropped
		}
		r.handleMsgPropose(m)
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
		if r.isolates[m.From] == true {
			r.isolates[m.From] = false
		}
		r.sendAppend(m.From)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

// stepFollower step the message as a follower state
func (r *Raft) stepFollower(m pb.Message) error{
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendRequestVote(peer)
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
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
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
	log.Infof("Candidate %d, vote count %d, sum %d, reject %d", r.id, voteCount, sumPeer, rejectCount)
	if voteCount > sumPeer/2 {
		//get agree half above , the node become leader and send heartbeat immediately
		r.becomeLeader()
		r.sendHeartbeat()
	} else if rejectCount > sumPeer/2 {
		//if reject half above, the candidate become follower immediately
		r.becomeFollower(r.Term, None)
	}
}

//handleMsgTimeoutNow handle the msg that current leader Timeout Request
func (r *Raft) handleMsgTimeoutNow(m pb.Message) {
	// if the node was removed, then skip the msg
	if r.checkNodeRemoved() {
		return
	}
	// if the node is new or already is candidate, just skip the msg
	if r.Prs[r.id] != nil || r.State != StateCandidate{
		log.Infof("timeout %d become candidate", r.id)
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.sendRequestVote(peer)
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	lastTerm,_ := r.RaftLog.Term(lastIndex)

	if lastIndex < meta.RaftInitLogIndex && lastTerm < meta.RaftInitLogTerm && len(r.Prs) == 0 {
		log.Infof("Peer %d need snapshot.", r.id)
		r.sendAppendResponse(m.From, lastTerm, lastIndex, true)
		return
	}
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
	// if node is removed, then ignore the msg
	log.Infof("peer %d transfer to peer %d", r.id, m.From)
	if r.State != StateLeader {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		return
	}

	if r.checkNodeRemoved() || m.From == r.id{
		return
	}

	peer,ok := r.Prs[m.From]
	if !ok{
		return
	}

	if r.leadTransferee == r.id {
		return
	}

	// the transfer request is a new request or the transferee in the raft node is stale
	r.leadTransferee  = m.From
	r.transferElapsed = 0
	if peer.Match == r.RaftLog.LastIndex() {
		log.Infof("transfer ok")
		r.transferElapsed = 0
		r.sendTimeout(m.From)
		return
	}
	log.Infof("transfer false")
	// to prove the leader transfer need help
	r.helpTransferee = true
	r.sendAppend(m.From)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// A heartbeat means the node add into the group, skip that
	if m.From == 0 {
		return
	}
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

func (r *Raft) handleMsgPropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		//TODO confirm the lastIndex + 1
		entry.Index = lastIndex + uint64(i) +1
		log.Infof("MsgPropose id:%v enrty: term index %v %v self index: %v\n",r.id, entry.Term,entry.Index,r.RaftLog.committed)
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
	if m.Index < meta.RaftInitLogIndex && m.LogTerm < meta.RaftInitLogTerm && m.Reject {
		log.Infof("Send snapshot to new node %d",m.From)
		r.sendSnapshot(m.From)
		return
	}
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
				log.Infof("Leader %d commit index to %d", r.id, n)
				r.RaftLog.committed = n
				for peer := range r.Prs {
					if peer == r.id {
						continue
					}
					r.sendAppend(peer)
				}
			}
		}
	}
	if r.Prs[m.From].Match >= r.pendingSnapshotIndex && r.Prs[m.From].snapState == SnapshotSending {
		r.Prs[m.From].snapState = SnapshotNoNeed
	}
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() && r.helpTransferee == true{
		r.helpTransferee = false
		msg := pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
		}
		r.msgs = append(r.msgs, msg)
		r.transferElapsed = 0
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
	term,_ := r.RaftLog.Term(metaData.Term)
	/*
	if r.RaftLog.committed >= metaData.Index {
	 */
	if term == metaData.Term {
		r.sendSnapshotResponse(m.From, r.RaftLog.committed)
		return
	}
	r.Prs = make(map[uint64]*Progress)
	r.isolates = make(map[uint64]bool)
	// if node receive a snapshot, means this node is not a leader
	// so reset the votes
	r.votes = make(map[uint64]bool)
	for _, peer := range metaData.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
		r.isolates[peer] =false
		r.votes[peer] = false
	}
	log.Infof("Peer %d receive the snapshot, committed,stabled,applied to %d", r.id, metaData.Index)
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
	log.Infof("peer %d add node %d", r.id, id)
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
		if r.State == StateLeader {
			r.sendAppend(id)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				To: id,
				From: r.id,
				Term: r.Term,
			})
		}
	}
	r.PendingConfIndex = None
	return
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	log.Infof("peer %d remove node %d", r.id, id)
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

// checkNodeRemoved check whether node itself could be removed
func (r *Raft) checkNodeRemoved() bool {
	if _,ok := r.Prs[r.id]; ok {
		return false
	}
	return true
}


// checkIsolate check if the leader node is isolated by above n/2 nodes
func (r *Raft) checkIsolate() bool {
	sum := len(r.isolates)
	// r.isolates[r.id] = true
	counts := 0
	for peer := range r.isolates {
		if r.isolates[peer] == true {
			counts++
		}
	}
	if counts > sum / 2 {
		return true
	}
	return false
}

//get the proper follower if current leader need to be removed
func (r *Raft) GetProperFollower() uint64 {
	if r.State != StateLeader {
		log.Infof("%d Choose follower but not the leader!", r.id)
		return 0
	}
	if len(r.Prs) <= 1 {
		return 0
	}
	maxMatch := uint64(0)
	maxMatchID := uint64(0)
	for peer := range r.Prs {
		if peer != r.id {
			if r.Prs[peer].Match >= maxMatch {
				maxMatch = r.Prs[peer].Match
				maxMatchID = peer
			}
		}
	}
	return maxMatchID
}
