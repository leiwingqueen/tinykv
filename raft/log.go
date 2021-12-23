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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		entries:         make([]pb.Entry, 0),
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		//在entries中能查找到
		offset := l.entries[0].Index
		if i >= offset && i <= offset+uint64(len(l.entries))-1 {
			return l.entries[i-offset].Term, nil
		}
	}
	//未持久化的snapshot
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	//都找不到只能去storage查找
	return l.storage.Term(i)
}

//---------------下面是自行加的方法------------------

func (l *RaftLog) slice(start uint64) []*pb.Entry {
	if start > l.LastIndex() {
		panic("idx err...")
	}
	if start <= l.stabled {
		panic("idx err")
	}
	idx := start - l.stabled - 1
	arr := make([]*pb.Entry, 0)
	for _, log := range l.entries[idx:] {
		arr = append(arr, &log)
	}
	return arr
}

func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) bool {
	term, _ := l.Term(index)
	if term != logTerm {
		return false
	}
	l.append(ents...)
	l.committed = committed
	return true
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	//更新/覆盖本地日志，不能直接删除后面的日志
	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
	//and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.
	idx := ents[0].Index
	for _, entry := range ents {
		//覆盖
		if idx <= l.LastIndex() {
			if term, _ := l.Term(idx); term != entry.Term {
				DPrintf("log conflict,delete log...delete start:%d", idx)
				l.trimLast(idx)
				l.entries = append(l.entries, *entry)
			}
		} else {
			l.entries = append(l.entries, *entry)
		}
		idx++
	}
	return l.LastIndex()
}

func (l *RaftLog) trimLast(idx uint64) {
	offset := l.entries[0].Index
	l.entries = l.entries[idx-offset:]
}

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}
