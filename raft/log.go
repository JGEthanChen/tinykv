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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//record the first index, which is set as offset
	firstOffset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex,err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	firstIndex,err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	entries,err := storage.Entries(firstIndex, lastIndex+1) //begin with the entry persisted in storage
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage: storage,
		applied: firstIndex-1,
		stabled: lastIndex,
		entries: entries,
		firstOffset: firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndex,_ := l.storage.FirstIndex()
	if firstIndex > l.firstOffset {
		if len(l.entries) > 0 {
			entries := l.entries[int(firstIndex-l.firstOffset):]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.firstOffset = firstIndex
	}
}

// Entries return one entry slice from [lo,hi)
func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry,error) {
	if lo > hi || hi > l.LastIndex()+1 {
		return nil, ErrCompacted
	}
	if lo < l.firstOffset {
		return nil, ErrCompacted
	}
	ents := make([]pb.Entry, 0)
	if hi > l.firstOffset {
		ents = append(ents, l.entries[max(lo, l.firstOffset)-l.firstOffset: hi-l.firstOffset]...)

	}
	return ents, nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstOffset+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	if l.applied == l.committed {
		return nil
	}
	// log.Infof("nextents applied %d commited %d firstoffset %d", l.applied, l.committed, l.firstOffset)
	return l.entries[l.applied-l.firstOffset+1 : l.committed - l.firstOffset + 1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	entsLen := len(l.entries)
	if entsLen > 0 {
		return l.firstOffset + uint64(entsLen) -1
	} else {
		// If the log entry is empty, get the storage last index
		lastIndex,_ := l.storage.LastIndex()
		if IsEmptySnap(l.pendingSnapshot)==false && l.pendingSnapshot.Metadata.Index > lastIndex {
			lastIndex = l.pendingSnapshot.Metadata.Index
		}
		return lastIndex
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if len(l.entries) > 0 && i >= l.firstOffset && i <= l.LastIndex(){
		// case that the entry is excepted
		return l.entries[i-l.firstOffset].Term,nil
	}
	term, err := l.storage.Term(i)
	if err != nil && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			return term, ErrCompacted
		}
	}
	return term, err
}

func (l *RaftLog) findMatchIndex(last, logTerm uint64) uint64{
	curIndex := min(l.LastIndex(), last)
	for ;curIndex > l.committed; curIndex-- {
		term, err := l.Term(curIndex)
		if term != logTerm || err != nil {
			break
		}
	}
	return curIndex
}

func (l *RaftLog) Append(entries []*pb.Entry) {
	for i, entry := range entries {
		logTerm, err := l.Term(entry.Index)
		if err != nil || logTerm != entry.Term {
			idx := int(entry.Index-l.entries[0].Index)
			l.entries = l.entries[:idx]
			for j,ent := range entries {
				if j >= i {
					l.entries = append(l.entries, *ent)
				}
			}
			l.stabled = min(l.stabled, entry.Index-1)
			break
		}
	}
}

