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
	"log"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

	/***************下面是自定义的属性***********************/
	//随机选举超时，为了避免live lock的问题
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftlog := newLog(c.Storage)
	//这里相当于论文定义的next index和match index
	//TODO:这里需要读持久化的数据
	prs := make(map[uint64]*Progress)
	for _, peerId := range c.peers {
		if peerId != c.ID {
			prs[peerId] = &Progress{0, 1}
		}
	}
	raft := Raft{
		id: c.ID,
		//TODO:读持久化存储
		Term:             0,
		Vote:             0,
		RaftLog:          raftlog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	raft.resetRandomElectionTimeout()
	return &raft
}

//更新随机超时时间
func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	DPrintf("log replicate...from:%d,to:%d", r.id, to)
	progress := r.Prs[to]
	preLogIdx := progress.Next - 1
	preLogTerm, _ := r.RaftLog.Term(preLogIdx)
	entries := r.RaftLog.slice(progress.Next, r.RaftLog.LastIndex()+1)
	ent := make([]*pb.Entry, 0)
	for _, entry := range entries {
		ent = append(ent, &entry)
	}
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//下面这两个其实应该命名为PreLogTerm和PreLogIndex
		LogTerm:  preLogTerm,
		Index:    preLogIdx,
		Entries:  ent,
		Commit:   r.RaftLog.committed,
		Snapshot: nil,
	}
	r.send(message)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	DPrintf("send heartbeat...from:%d,to:%d,msg:%+v", msg.From, msg.To, msg)
	r.send(msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	DPrintf("send request vote...from:%d,to:%d,term:%d", msg.From, msg.To, msg.Term)
	r.send(msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++
	//选举超时
	if r.electionElapsed >= r.randomElectionTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		r.electionElapsed = 0
		return
	}
	//心跳定时器
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
		})
		r.heartbeatElapsed = 0
		r.electionElapsed = 0
	}
}

func (r *Raft) handleElectionTimeout() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	for peerId := range r.Prs {
		r.sendRequestVote(peerId)
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	DPrintf("role change...peerId:%d,term:%d,role:Follower", r.id, term)
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = lead
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	DPrintf("role change...peerId:%d,role:Candidate", r.id)
	r.State = StateCandidate
	r.electionElapsed = 0
	r.Term++
	//直接投自己一票
	r.Vote = r.id
	r.votes[r.id] = true
	r.stepCandidate(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
	})
	r.resetRandomElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	DPrintf("role change...peerId:%d,role:Leader", r.id)
	r.Lead = r.id
	r.State = StateLeader
	//写NOOP
	emptyEnt := pb.Entry{Data: nil}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&emptyEnt}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgBeat:
		for peerId := range r.Prs {
			if peerId != r.id {
				r.sendHeartbeat(peerId)
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleMsgAppendResponse(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)
	case pb.MessageType_MsgHup:
		r.handleElectionTimeout()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleElectionTimeout()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

//写入log到本地
func (r *Raft) handleMsgPropose(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	r.appendEntry(m.Entries)
	//同步log到其他结点
	for peerId := range r.Prs {
		if peerId != r.id {
			r.sendAppend(peerId)
		}
	}
}

func (r *Raft) appendEntry(es []*pb.Entry) {
	//统一生成term和index
	last := r.RaftLog.LastIndex()
	for i, ent := range es {
		ent.Term = r.Term
		ent.Index = last + 1 + uint64(i)
	}
	DPrintf("append entry...peerId:%d,es:+%v", r.id, es)
	r.RaftLog.append(es...)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("handle log replicate...peerId:%d,from:%d,msg:+%v", r.id, m.From, m)
	if m.Term < r.Term {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			Index:   m.Index,
			Reject:  true,
		})
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			Index:   m.Index,
			Reject:  false,
		})
		return
	}
	//并发/重试导致commit idx已经更新
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}
	if ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		if r.State != StateFollower {
			r.becomeFollower(m.Term, m.From)
		}
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index})
	} else {
		//这里还需要有加速冲突解决的逻辑
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   m.Index,
			Reject:  true,
		})
	}
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	DPrintf("handle msg append response...peerId:%d,msg:%+v", r.id, m)
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	if !m.Reject {
		progress := r.Prs[m.From]
		//delay msg
		if m.Index <= progress.Match {
			return
		}
		//更新match index和next index
		progress.Match = m.Index
		progress.Next = progress.Match + 1
		//检查其他节点的match index，判断是否需要更新commit
		mp := make(map[uint64]int)
		for _, p := range r.Prs {
			mp[p.Match]++
		}
		peerSize := len(r.Prs) + 1
		//figure 8里面有提到，更新commit的term必须是当前leader的任期
		for idx, cnt := range mp {
			//超过半数结点同步成功则更新commit
			if logTerm, _ := r.RaftLog.Term(idx); cnt+1 > peerSize/2 && idx > r.RaftLog.committed &&
				//figure 8要求的内容
				logTerm == r.Term {
				DPrintf("update commit idx...peerId:%d,commitIdx:%d", r.id, idx)
				r.RaftLog.committed = idx
				//更新unstable
				//r.RaftLog.stabled = idx
				//r.RaftLog.entries = r.RaftLog.slice(idx+1, r.RaftLog.LastIndex()+1)
			}
		}
	} else {
		//TODO:日志冲突的场景
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	defer func() {
		r.msgs = append(r.msgs, resp)
	}()
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(r.Term, m.From)
		return
	}
	//term相等的场景下先不处理
}

func (r *Raft) handleHeartbeatResp(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(r.Term, m.From)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	defer func() {
		r.send(resp)
	}()
	if r.Term > m.Term {
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
		r.Vote = m.From
		resp.Term = r.Term
		resp.Reject = false
		return
	}
	//term相同的情况下需要判断是否已经投票
	if r.Vote <= 0 || r.Vote == m.From {
		r.Vote = m.From
		resp.Term = r.Term
		resp.Reject = false
		return
	}
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	DPrintf("handle request vote resp...msg:%+v,term:%d", m, r.Term)
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	r.votes[m.From] = !m.Reject
	if !m.Reject {
		cnt := 0
		for _, v := range r.votes {
			if v {
				cnt++
			}
		}
		if cnt > (len(r.Prs)+1)/2 {
			r.becomeLeader()
		}
	}
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

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}
