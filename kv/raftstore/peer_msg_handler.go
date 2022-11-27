package raftstore

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()

		// Persist snapshot, raft log, hardState, metadata etc
		applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			log.Panicf("%s save raft ready state error %v", d.Tag, err)
		}
		if applySnapResult != nil {
			d.updateStoreMeta(applySnapResult.Region)
		}

		// Send messages to other peer
		d.Send(d.ctx.trans, ready.Messages)

		// Apply committed entries
		for i, size := 0, len(ready.CommittedEntries); i < size; i++ {
			d.applyCommittedEntry(&ready.CommittedEntries[i])
			if d.stopped {
				return
			}
		}

		// Persist applied index
		if len(ready.CommittedEntries) > 0 {
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			if err = engine_util.PutMeta(d.ctx.engine.Kv, meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
				log.Panic(err)
			}
		}

		// Advance progress
		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) applyCommittedEntry(entry *eraftpb.Entry) {
	if entry.Data == nil {
		return
	}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		var cc = new(eraftpb.ConfChange)
		if err := proto.Unmarshal(entry.Data, cc); err != nil {
			d.handleProposal(entry, ErrResp(err))
			log.Panicf("%s apply conf change error %v", d.Tag, err)
		}
		d.applyConfChange(cc, entry)
		return
	}

	var req = new(raft_cmdpb.RaftCmdRequest)
	if err := proto.Unmarshal(entry.Data, req); err != nil {
		d.handleProposal(entry, ErrResp(err))
		log.Panicf("%s apply raft committed entry error %v", d.Tag, err)
	}
	if req.AdminRequest != nil {
		d.applyAdminRequest(req, entry)
	} else {
		d.applyRequest(req, entry)
	}
}

func (d *peerMsgHandler) applyConfChange(cc *eraftpb.ConfChange, entry *eraftpb.Entry) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := proto.Unmarshal(cc.Context, msg); err != nil {
		d.handleProposal(entry, ErrResp(err))
		log.Panicf("%s apply conf change error %v", d.Tag, err)
	}
	region := d.Region()
	if err, ok := util.CheckRegionEpoch(msg, region, true).(*util.ErrEpochNotMatch); ok {
		log.Debugf("[applyConfChange] %s RegionEpoch not match", d.Tag)
		d.handleProposal(entry, ErrResp(err))
		return
	}
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// The peer not exists, add it
		if d.indexOfPeer(cc.NodeId) == -1 {
			region.Peers = append(region.Peers, msg.AdminRequest.ChangePeer.Peer)
			region.RegionEpoch.ConfVer++
			d.updateRegionLocalState(region)
			d.updateStoreMeta(region)
			d.insertPeerCache(msg.AdminRequest.ChangePeer.Peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.PeerId() {
			if d.MaybeDestroy() {
				d.destroyPeer()
			}
			return
		}
		// The peer exists, remove it
		if i := d.indexOfPeer(cc.NodeId); i != -1 {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			region.RegionEpoch.ConfVer++
			d.updateRegionLocalState(region)
			d.updateStoreMeta(region)
			d.removePeerCache(cc.NodeId)
		}
	}

	d.RaftGroup.ApplyConfChange(*cc)

	clonedRegion := new(metapb.Region)
	if err := util.CloneMsg(region, clonedRegion); err != nil {
		d.handleProposal(entry, ErrResp(err))
		log.Panicf("%s apply conf change error %v", d.Tag, err)
	}
	d.handleProposal(entry, &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: clonedRegion},
		},
	})

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) applyRequest(cmd *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry) {
	region := d.Region()
	if errEpochNotMatch, ok := util.CheckRegionEpoch(cmd, region, true).(*util.ErrEpochNotMatch); ok {
		log.Debugf("[applyRequest] %s RegionEpoch not match", d.Tag)
		d.handleProposal(entry, ErrResp(errEpochNotMatch))
		return
	}
	kvWB := new(engine_util.WriteBatch)
	var responses []*raft_cmdpb.Response
	for _, req := range cmd.Requests {
		response := &raft_cmdpb.Response{CmdType: req.CmdType}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			if err := util.CheckKeyInRegion(req.Get.Key, region); err != nil {
				d.handleProposal(entry, ErrResp(err))
				return
			}
			val, err := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				d.handleProposal(entry, ErrResp(err))
				log.Panicf("%s apply get cmd error %v", d.Tag, err)
			}
			response.Get = &raft_cmdpb.GetResponse{Value: val}
		case raft_cmdpb.CmdType_Put:
			if err := util.CheckKeyInRegion(req.Put.Key, region); err != nil {
				d.handleProposal(entry, ErrResp(err))
				return
			}
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			response.Put = &raft_cmdpb.PutResponse{}
			d.SizeDiffHint += uint64(len(engine_util.KeyWithCF(req.Put.Cf, req.Put.Key)) + len(req.Put.Value))
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckKeyInRegion(req.Delete.Key, region); err != nil {
				d.handleProposal(entry, ErrResp(err))
				return
			}
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			response.Delete = &raft_cmdpb.DeleteResponse{}
			d.SizeDiffHint -= uint64(len(engine_util.KeyWithCF(req.Delete.Cf, req.Delete.Key)))
		case raft_cmdpb.CmdType_Snap:
			clonedRegion := new(metapb.Region)
			if err := util.CloneMsg(region, clonedRegion); err != nil {
				d.handleProposal(entry, ErrResp(err))
				log.Panicf("%s apply snap command entry error %v", d.Tag, err)
			}
			response.Snap = &raft_cmdpb.SnapResponse{Region: clonedRegion}
		}
		responses = append(responses, response)
	}
	if err := kvWB.WriteToDB(d.ctx.engine.Kv); err != nil {
		d.handleProposal(entry, ErrResp(err))
		log.Panicf("%s apply kv command entry error %v", d.Tag, err)
	}
	d.handleProposal(entry, &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}, Responses: responses})
}

func (d *peerMsgHandler) applyAdminRequest(req *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry) {
	adminReq := req.AdminRequest
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactTerm, compactIndex := adminReq.CompactLog.CompactTerm, adminReq.CompactLog.CompactIndex
		currentTruncatedTerm, currentTruncatedIndex := d.peerStorage.truncatedTerm(), d.peerStorage.truncatedIndex()
		if compactTerm > currentTruncatedTerm || compactTerm == currentTruncatedTerm && compactIndex > currentTruncatedIndex {
			applyState := d.peerStorage.applyState
			applyState.TruncatedState.Term = compactTerm
			applyState.TruncatedState.Index = compactIndex
			if err := engine_util.PutMeta(d.ctx.engine.Kv, meta.ApplyStateKey(d.regionId), applyState); err != nil {
				log.Panicf("%s write apply state error %v", d.Tag, err)
			}
			d.ScheduleCompactLog(compactIndex)
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// Should not match because this command does not require a proposal
	case raft_cmdpb.AdminCmdType_ChangePeer:
	// Handled by other, see peerMsgHandler.applyConfChange()
	case raft_cmdpb.AdminCmdType_Split:
		if req.Header.RegionId != d.regionId {
			d.handleProposal(entry, ErrRespRegionNotFound(req.Header.RegionId))
			return
		}
		if errEpochNotMatch, ok := util.CheckRegionEpoch(req, d.Region(), true).(*util.ErrEpochNotMatch); ok {
			log.Debugf("[applySplitRegion] %s RegionEpoch not match", d.Tag)
			d.handleProposal(entry, ErrResp(errEpochNotMatch))
			return
		}
		splitReq := adminReq.Split
		if err := util.CheckKeyInRegion(splitReq.SplitKey, d.Region()); err != nil {
			d.handleProposal(entry, ErrResp(err))
			return
		}
		if len(splitReq.NewPeerIds) != len(d.Region().Peers) {
			d.handleProposal(entry, ErrRespStaleCommand(d.Term()))
			return
		}

		currentRegion := d.Region()
		currentRegion.RegionEpoch.Version++

		var peers []*metapb.Peer
		for i, peedId := range splitReq.NewPeerIds {
			peers = append(peers, &metapb.Peer{
				Id:      peedId,
				StoreId: currentRegion.Peers[i].StoreId,
			})
		}
		newRegion := &metapb.Region{
			Id:       splitReq.NewRegionId,
			StartKey: splitReq.SplitKey,
			EndKey:   currentRegion.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: InitEpochConfVer,
				Version: InitEpochVer,
			},
			Peers: peers,
		}

		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{region: currentRegion})
		currentRegion.EndKey = splitReq.SplitKey
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: currentRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		storeMeta.regions[newRegion.Id] = newRegion
		storeMeta.Unlock()

		// 持久化 currentRegion 和 newRegion
		kvWB := new(engine_util.WriteBatch)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, currentRegion, rspb.PeerState_Normal)
		kvWB.MustWriteToDB(d.ctx.engine.Kv)

		// 创建当前 store 上的 newRegion Peer，注册到 router，并启动
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			d.handleProposal(entry, ErrResp(err))
			log.Panicf("[splitRegion]%s create peer error %v", d.Tag, err)
		}
		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

		d.handleProposal(entry, &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{currentRegion, newRegion}},
			},
		})
		log.Debugf("[AdminCmdType_Split Process] currentRegion %v, newRegion %v", currentRegion, newRegion)
		// 发送 heartbeat 给其他节点
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			d.notifyHeartbeatScheduler(newRegion, newPeer)
		}
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := proto.Marshal(msg)
		if err != nil {
			log.Errorf("%s propose compact log error %v", d.Tag, err)
			return
		}
		if err = d.RaftGroup.Propose(data); err != nil {
			log.Errorf("%s propose compact log error %v", d.Tag, err)
		}
		// No callback
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.peerStorage.AppliedIndex() < d.RaftGroup.Raft.PendingConfIndex {
			cb.Done(ErrResp(errors.Errorf("Already exists a pending conf change with index %d", d.RaftGroup.Raft.PendingConfIndex)))
			return
		}
		changePeer := msg.AdminRequest.ChangePeer
		if changePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
			len(d.Region().Peers) == 2 && changePeer.Peer.Id == d.PeerId() {
			for _, peer := range d.Region().Peers {
				if peer.Id != d.PeerId() {
					d.RaftGroup.TransferLeader(peer.Id)
					break
				}
			}
		}

		context, err := proto.Marshal(msg)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		index := d.nextProposalIndex()
		cc := eraftpb.ConfChange{ChangeType: changePeer.ChangeType, NodeId: changePeer.Peer.Id, Context: context}
		if err = d.RaftGroup.ProposeConfChange(cc); err != nil {
			if err == raft.ErrProposalDropped {
				cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId}))
				return
			}
			cb.Done(ErrResp(err))
			return
		}
		d.proposals = append(d.proposals, &proposal{index: index, term: d.Term(), cb: cb})
	case raft_cmdpb.AdminCmdType_Split:
		if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := proto.Marshal(msg)
		if err != nil {
			log.Errorf("%s propose split region error %v", d.Tag, err)
			cb.Done(ErrResp(err))
			return
		}
		index := d.nextProposalIndex()
		if err = d.RaftGroup.Propose(data); err != nil {
			log.Errorf("%s propose split region error %v", d.Tag, err)
			if err == raft.ErrProposalDropped {
				cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId}))
				return
			}
			cb.Done(ErrResp(err))
			return
		}
		d.proposals = append(d.proposals, &proposal{index: index, term: d.Term(), cb: cb})
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	var key []byte
	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		default:
			continue
		}
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	index := d.peer.nextProposalIndex()
	if err = d.RaftGroup.Propose(data); err != nil {
		if err == raft.ErrProposalDropped {
			cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId}))
			return
		}
		cb.Done(ErrResp(err))
		return
	}
	d.proposals = append(d.proposals, &proposal{index: index, term: d.Term(), cb: cb})
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) updateRegionLocalState(region *metapb.Region) {
	kvWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	kvWB.MustWriteToDB(d.ctx.engine.Kv)
}

func (d *peerMsgHandler) updateStoreMeta(region *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	meta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	meta.setRegion(region, d.peer)
}

func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	if len(d.proposals) == 0 {
		return
	}
	var proposalIndex = -1
	for i, proposal := range d.proposals {
		if proposal.term < entry.Term {
			NotifyStaleReq(d.Term(), proposal.cb)
			proposalIndex = i
		} else if proposal.index == entry.Index && proposal.term == entry.Term {
			if len(resp.Responses) == 1 && resp.Responses[0].Snap != nil {
				proposal.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			}
			proposal.cb.Done(resp)
			proposalIndex = i
			break
		}
	}
	if proposalIndex != -1 {
		d.proposals = d.proposals[proposalIndex+1:]
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		log.Panicf("%s send heartbeat error %v", d.Tag, err)
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) indexOfPeer(peerId uint64) int {
	for i, p := range d.Region().Peers {
		if p.Id == peerId {
			return i
		}
	}
	return -1
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
