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
	"os"
	"sort"
	"sync"
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

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{rand: rand.New(rand.NewSource(time.Now().UnixNano()))}

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
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	majority                  int
	randomizedElectionTimeout int
	logger                    *log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog.committed = hs.Commit

	var peers []uint64
	if c.peers != nil {
		peers = c.peers
	} else {
		peers = cs.Nodes
	}
	if len(peers) == 0 {
		peers = append(peers, c.ID)
	}
	prs := make(map[uint64]*Progress)
	for _, id := range peers {
		prs[id] = new(Progress)
	}
	r := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		majority:         len(peers)/2 + 1,
		logger:           log.NewLogger(os.Stdout, ""),
	}
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next
	if nextIndex <= r.RaftLog.dummyIndex {
		// Send snapshot
		r.msgs = append(r.msgs, pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			LogTerm:  0,
			Index:    0,
			Commit:   0,
			Snapshot: nil,
		})
		return true
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		r.logger.Debugf("error occurred during sending append: %v", err)
		return false
	}
	ents := r.RaftLog.entriesStartAt(nextIndex)
	entries := make([]*pb.Entry, 0, len(ents))
	for i, size := 0, len(ents); i < size; i++ {
		entries = append(entries, &ents[i])
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
			r.logger.Debugf("error occurred during election: %v", err)
		}
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
			r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("Cannot directly convert from leader to candidate")
	}
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	for id, pr := range r.Prs {
		pr.Match = 0
		pr.Next = r.RaftLog.LastIndex() + 1
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}

	// Propose a noop entry
	if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}}); err != nil {
		r.logger.Debugf("error occurred during proposing a noop entry: %v", err)
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = map[uint64]bool{}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) (err error) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return
		}
		r.votes[m.From] = !m.Reject
		count := 0
		for _, granted := range r.votes {
			if granted {
				count++
			}
			if count >= r.majority {
				r.becomeLeader()
				return
			}
		}
		if len(r.votes) == len(r.Prs) {
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendHeartbeat(id)
		}
	case pb.MessageType_MsgPropose:
		r.appendEntry(m.Entries...)
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		if r.standalone() {
			r.RaftLog.committed = r.RaftLog.LastIndex()
			return
		}
		r.broadcastAppend()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return
		}
		if m.LogTerm < r.RaftLog.lastTerm() || m.LogTerm == r.RaftLog.lastTerm() && m.Index < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return
		}
		if !m.Reject {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			if r.updateLeaderCommit() {
				r.broadcastAppend()
			}
			return
		}
		conflictTerm := m.LogTerm
		if conflictTerm != 0 {
			for i, dummyIndex := r.RaftLog.LastIndex(), r.RaftLog.dummyIndex; i >= dummyIndex; i-- {
				term, _ := r.RaftLog.Term(i)
				if term == conflictTerm {
					r.Prs[m.From].Next = i + 1
					return
				}
				if term < conflictTerm {
					break
				}
			}
		}
		r.Prs[m.From].Next = m.Index
		r.sendAppend(m.From)
	}
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	if r.standalone() {
		r.becomeLeader()
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.lastTerm(),
			Index:   r.RaftLog.LastIndex(),
		})
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Reject:  true,
	}
	defer func() {
		resp.Term = r.Term
		r.msgs = append(r.msgs, resp)
	}()

	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if (r.Vote == None || r.Vote == m.From) && (m.LogTerm > r.RaftLog.lastTerm() ||
		m.LogTerm == r.RaftLog.lastTerm() && m.Index >= r.RaftLog.LastIndex()) {
		r.Vote = m.From
		resp.Reject = false
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		LogTerm: 0,
		Index:   0,
		Reject:  true,
	}
	defer func() {
		resp.Term = r.Term
		r.msgs = append(r.msgs, resp)
	}()

	if m.Term < r.Term || m.Index < r.RaftLog.dummyIndex {
		return
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	// If a follower does not have prevLogIndex in its log,
	// it should return with conflictIndex = len(log) and conflictTerm = None.
	if m.Index > r.RaftLog.LastIndex() {
		resp.Index = r.RaftLog.LastIndex() + 1
		return
	}

	// If a follower does have prevLogIndex in its log, but the term does not match,
	// it should return conflictTerm = log[prevLogIndex].Term, and then search its log
	// for the first index whose entry has term equal to conflictTerm.
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err)
	}
	if logTerm != m.LogTerm {
		conflictTerm := logTerm
		var conflictIndex uint64
		for i := r.RaftLog.dummyIndex; i <= m.Index; i++ {
			if logTerm, _ = r.RaftLog.Term(i); logTerm == conflictTerm {
				conflictIndex = i
				break
			}
		}
		resp.Index = conflictIndex
		resp.LogTerm = conflictTerm
		return
	}

	for i, entry := range m.Entries {
		if entry.Index > r.RaftLog.LastIndex() {
			r.RaftLog.append(m.Entries[i:]...)
			break
		}
		if term, _ := r.RaftLog.Term(entry.Index); term != entry.Term {
			r.RaftLog.deleteFrom(entry.Index)
			r.RaftLog.append(m.Entries[i:]...)
			break
		}
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	resp.Reject = false
	resp.Index = r.RaftLog.LastIndex()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	}
	defer func() {
		resp.Term = r.Term
		r.msgs = append(r.msgs, resp)
	}()

	if m.Term < r.Term {
		return
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	resp.Index = r.RaftLog.LastIndex()
	resp.LogTerm = r.RaftLog.lastTerm()
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) appendEntry(ents ...*pb.Entry) {
	offset := r.RaftLog.LastIndex() + 1
	for i, ent := range ents {
		ent.Index = offset + uint64(i)
		ent.Term = r.Term
	}
	r.RaftLog.append(ents...)
}

func (r *Raft) standalone() bool {
	return len(r.Prs) == 1 && r.Prs[r.id] != nil
}

func (r *Raft) updateLeaderCommit() bool {
	matchIndex := make(uint64Slice, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matchIndex = append(matchIndex, pr.Match)
	}
	sort.Sort(matchIndex)

	var n uint64
	length := uint64(len(matchIndex))
	if length%2 == 0 {
		n = matchIndex[length>>1-1]
	} else {
		n = matchIndex[length>>1]
	}
	if logTerm, _ := r.RaftLog.Term(n); n > r.RaftLog.committed && logTerm == r.Term {
		r.RaftLog.committed = n
		return true
	}
	return false
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}
