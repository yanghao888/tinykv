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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	// dummyIndex is equal to the index last included in the snapshot
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	truncatedIndex := firstIndex - 1
	truncatedTerm, err := storage.Term(truncatedIndex)
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:    storage,
		committed:  truncatedIndex,
		applied:    truncatedIndex,
		stabled:    lastIndex,
		entries:    append([]pb.Entry{{Term: truncatedTerm, Index: truncatedIndex}}, entries...),
		dummyIndex: truncatedIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	newFirstIndex, _ := l.storage.FirstIndex()
	newTruncatedIndex := newFirstIndex - 1
	if newTruncatedIndex > l.dummyIndex {
		l.entries = append([]pb.Entry{}, l.entries[newTruncatedIndex-l.dummyIndex:]...)
		l.dummyIndex = newTruncatedIndex
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[l.stabled+1-l.dummyIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied+1-l.dummyIndex : l.committed+1-l.dummyIndex]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, errors.Errorf("Index is out of bound, lastIndex %d", l.LastIndex())
	}
	if i < l.dummyIndex {
		return 0, ErrCompacted
	}
	return l.entries[i-l.dummyIndex].Term, nil
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
	return l.LastIndex()
}

func (l *RaftLog) lastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) entriesStartAt(index uint64) []pb.Entry {
	if index <= l.dummyIndex {
		panic("index is unavailable due to compaction")
	}
	return l.entries[index-l.dummyIndex:]
}

func (l *RaftLog) deleteFrom(index uint64) {
	if index <= l.dummyIndex {
		panic("index is unavailable due to compaction")
	}
	l.entries = l.entries[:index-l.dummyIndex]
	l.stabled = min(l.stabled, l.LastIndex())
}
