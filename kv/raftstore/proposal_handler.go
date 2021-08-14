package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"time"
)

type proposalHandler struct {
	*peerMsgHandler
	//committed entries that need to handle
	Entries []pb.Entry
}


func newProposalHandler(msgHandler *peerMsgHandler, committedEntries []pb.Entry) *proposalHandler {
	return &proposalHandler{
		peerMsgHandler: msgHandler,
		Entries: committedEntries,
	}
}


// Handle the proposal, main function in handler
func (h *proposalHandler)HandleProposal() {
	entrySize := len(h.Entries)
	if entrySize == 0  {
		return
	}
	kvWB := new(engine_util.WriteBatch)
	for _,entry := range h.Entries {
		log.Infof("%s process index %d term %d",h.Tag, entry.Index, entry.Term)

		msg := &raft_cmdpb.RaftCmdRequest{}
		if entry.GetData() == nil {
			continue
		} else if entry.EntryType != pb.EntryType_EntryConfChange{
			if err := msg.Unmarshal(entry.GetData());err != nil {
				panic(err)
			}
		}

		switch {
		case h.stopped == true:
			h.notifyStale(entry)
			continue
		case entry.EntryType == pb.EntryType_EntryConfChange:
			h.processConfChange(entry)
		case msg.AdminRequest != nil:
			h.processAdminRequest(msg, entry)
		case len(msg.Requests) > 0:
			h.processNormalRequest(msg, &entry)
		}
		// if stopped here, means the request is destroy peer itself
		if h.stopped == true {
			continue
		}
		log.Infof(" %s Applied Index %d", h.Tag, entry.Index)
		h.peerStorage.applyState.AppliedIndex = entry.Index
		kvWB.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
		kvWB.WriteToDB(h.peerMsgHandler.peerStorage.Engines.Kv)
		kvWB.Reset()
	}
	return
}

// processAddNode process the add node command in ConfChange
func (h *proposalHandler) processAddNode(confChange pb.ConfChange, resp *raft_cmdpb.RaftCmdResponse, cb *message.Callback) {
	region := &metapb.Region{}
	util.CloneMsg(h.Region(), region)

	reqCMD := &raft_cmdpb.RaftCmdRequest{}
	if err := reqCMD.Unmarshal(confChange.Context); err != nil {
		panic(err)
	}
	changePeer := reqCMD.GetAdminRequest().GetChangePeer().GetPeer()
	// the node exists, skip
	peer := util.FindPeer(region, changePeer.GetStoreId())
	if peer != nil {
		h.bindError(&util.ErrStaleCommand{}, cb, resp)
		return
	}

	// update the region
	region.Peers = append(region.Peers, changePeer)
	region.RegionEpoch.ConfVer++
	kvWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	if err := kvWB.WriteToDB(h.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}
	h.insertPeerCache(changePeer)
	h.setRegionWithLock(region)
	if h.IsLeader() {
		h.PeersStartPendingTime[changePeer.Id] = time.Now()
	}
	if cb != nil {
		cb.Done(resp)
	}
	// log.Infof
	log.Infof("%s region: %d, add Peer %d in peerCache.\n", h.Tag, h.regionId, changePeer.Id)
	fmt.Printf("%s region: %d, add Peer %d in peerCache.\n", h.Tag, h.regionId, changePeer.Id)
}

// processRemoveNode process the remove node command in ConfChange
func (h *proposalHandler) processRemoveNode(confChange pb.ConfChange, resp *raft_cmdpb.RaftCmdResponse, cb *message.Callback) {
	region := &metapb.Region{}
	util.CloneMsg(h.Region(), region)

	reqCMD := &raft_cmdpb.RaftCmdRequest{}
	if err := reqCMD.Unmarshal(confChange.Context); err != nil {
		panic(err)
	}

	// peer has already removed
	removePeer := reqCMD.GetAdminRequest().GetChangePeer().GetPeer()
	if util.RemovePeer(region, removePeer.GetStoreId()) == nil {
		h.bindError(&util.ErrStaleCommand{}, cb, resp)
		fmt.Printf("peer is already removed!\n")
		return
	}

	region.RegionEpoch.ConfVer++

	// remove itself
	if h.Meta.GetId() == confChange.NodeId {
		// refresh the region
		h.setRegionWithLock(region)
		h.destroyPeer()
		//log.Infof
		fmt.Printf("%s region %d peer %d remove itself already.\n", h.Tag, region.Id, removePeer.Id)
	} else {
		kvWB := new(engine_util.WriteBatch)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		if err := kvWB.WriteToDB(h.peerStorage.Engines.Kv); err != nil {
			panic(err)
		}
		h.removePeerCache(removePeer.Id)
		h.setRegionWithLock(region)
		if h.IsLeader() {
			delete(h.PeersStartPendingTime, removePeer.Id)
		}
		log.Infof("%s region %d peer %d remove already.\n", h.Tag, region.Id, removePeer.Id)
		fmt.Printf("%s region %d peer %d remove already.\n", h.Tag, region.Id, removePeer.Id)
	}
	if cb != nil {
		cb.Done(resp)
	}
	return
}

// processConfChange process the conf change entry
func (h *proposalHandler) processConfChange(entry pb.Entry) {
	confChange := pb.ConfChange{}
	if err := confChange.Unmarshal(entry.GetData()); err != nil {
		panic(err)
	}

	cb,_ := h.checkProposalCb(&entry)

	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
	}
	resp.AdminResponse.ChangePeer = &raft_cmdpb.ChangePeerResponse{Region: h.Region()}

	switch confChange.ChangeType {
	case pb.ConfChangeType_AddNode:
		h.processAddNode(confChange, resp, cb)
	case pb.ConfChangeType_RemoveNode:
		h.processRemoveNode(confChange, resp, cb)
	}

	h.RaftGroup.ApplyConfChange(confChange)

	if h.IsLeader() {
		h.HeartbeatScheduler(h.ctx.schedulerTaskSender)
	}
}



func (h *proposalHandler) processAdminRequest(msg *raft_cmdpb.RaftCmdRequest, entry pb.Entry) {
	log.Infof("[Region: %d] %d store: %d apply admin msg: %v",msg.Header.RegionId,msg.Header.Peer.Id, msg.Header.Peer.StoreId, msg.AdminRequest.CmdType)
	adReq := msg.AdminRequest

	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		h.processCompactLog(adReq, entry)
	case raft_cmdpb.AdminCmdType_Split:
		h.processSplit(msg,entry)
	}
}

func (h *proposalHandler) processSplit(msg *raft_cmdpb.RaftCmdRequest, entry pb.Entry) {
	splitMsg := msg.AdminRequest.Split

	cb,_ := h.checkProposalCb(&entry)

	resp := newCmdResp()
	region := &metapb.Region{}
	if err := util.CloneMsg(h.Region(), region); err != nil {
		panic("Region clone error!")
	}
	// If the split key is not in this region
	// Means the split key should be in (start key, end key)
	if err := util.CheckKeyInRegion(splitMsg.SplitKey, region); err != nil {//|| bytes.Compare(splitMsg.SplitKey, region.StartKey)  == 0 {
		log.Infof("%s Key not in region! key %s, region %d", h.Tag, string(splitMsg.SplitKey), region.Id)
		// TODO this case, should not return err resp?
		h.bindError(err, cb, resp)
		return
	}

	// peers not match
	if len(splitMsg.NewPeerIds) != len(region.Peers) {
		log.Infof("peer count not match in split msg")
		err := fmt.Errorf("peer count not match in split msg")
		h.bindError(err, cb, resp)
		return
	}
	region.RegionEpoch.Version++
	newRegion := h.createNewRegion(region, splitMsg)
	log.Infof("Create new region ID %d, cur Region %d, split key %s", newRegion.Id, region.Id, string(splitMsg.SplitKey))
	// prevRegion should adjust
	region.EndKey = util.SafeCopy(splitMsg.SplitKey)
	// persist into db
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	writeInitialApplyState(kvWB, region.Id)
	kvWB.WriteToDB(h.peerStorage.Engines.Kv)

	writeInitialRaftState(raftWB, region.Id)
	raftWB.WriteToDB(h.peerStorage.Engines.Raft)

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{region, newRegion},
		},
	}
	if cb != nil {
		cb.Done(resp)
	}
	h.SizeDiffHint = 0

	h.setRegionWithLock(region)
	h.ctx.storeMeta.Lock()
	h.ctx.storeMeta.regions[newRegion.Id] = newRegion
	h.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	h.ctx.storeMeta.Unlock()

	newPeer := h.createNewPeer(newRegion)

	// register the peer
	h.ctx.router.register(newPeer)
	h.ctx.router.send(newRegion.GetId(), message.Msg{RegionID: newRegion.GetId(), Type: message.MsgTypeStart})
	h.handlePendingVote(newRegion)

	h.ApproximateSize = nil
	if h.IsLeader() {
		h.HeartbeatScheduler(h.ctx.schedulerTaskSender)
		newPeer.HeartbeatScheduler(h.ctx.schedulerTaskSender)
		newPeer.MaybeCampaign(true)
	}
}

func (h *proposalHandler) handlePendingVote(region *metapb.Region) {
	for idx,m := range h.ctx.storeMeta.pendingVotes {
		if m.ToPeer.Id == h.Meta.Id && m.ToPeer.StoreId == h.Meta.StoreId {
			h.ctx.router.send(region.Id, message.Msg{Type: message.MsgTypeRaftMessage, Data: m})
			h.ctx.storeMeta.pendingVotes = append(h.ctx.storeMeta.pendingVotes[:idx], h.ctx.storeMeta.pendingVotes[idx+1:]...)
		}
	}
}

func (h *proposalHandler) createNewPeer(region *metapb.Region) *peer {
	newPeer, err := createPeer(h.ctx.store.Id, h.ctx.cfg, h.ctx.regionTaskSender, h.ctx.engine, region)
	if err != nil {
		log.Errorf("new peer create failed")
	}
	for _,peer := range region.Peers {
		newPeer.insertPeerCache(peer)
	}
	log.Infof(" %s create peer %s", h.Tag, newPeer.Tag)
	return newPeer
}

func (h *proposalHandler) createNewRegion(region *metapb.Region, splitMsg *raft_cmdpb.SplitRequest) *metapb.Region {
	peers := make([]*metapb.Peer, 0, len(splitMsg.NewPeerIds))
	for peerId,peer := range region.Peers {
		peers = append(peers, &metapb.Peer{
			Id: splitMsg.GetNewPeerIds()[peerId],
			StoreId: peer.GetStoreId(),
		})
	}
	newRegion := &metapb.Region{
		Id: splitMsg.GetNewRegionId(),
		StartKey: util.SafeCopy(splitMsg.SplitKey),
		EndKey: util.SafeCopy(region.EndKey),
		Peers: peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: InitEpochConfVer,
			Version: InitEpochVer,
		},
	}
	return newRegion
}


func(h *proposalHandler) processCompactLog(adReq *raft_cmdpb.AdminRequest, entry pb.Entry) {
	compactLog := adReq.GetCompactLog()
	kvWb := new(engine_util.WriteBatch)
	truncatedSt := h.peerStorage.applyState.TruncatedState
	//fmt.Printf("\ncompact log %v %v\n" , truncatedSt.Index, compactLog.CompactIndex)
	if truncatedSt.Index <= compactLog.CompactIndex {
		truncatedSt.Index = compactLog.CompactIndex
		truncatedSt.Term = compactLog.CompactTerm
		kvWb.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
		kvWb.WriteToDB(h.peerStorage.Engines.Kv)
		h.LastCompactedIdx = truncatedSt.Index + 1
		h.ctx.raftLogGCTaskSender <- &runner.RaftLogGCTask{
			RaftEngine: h.ctx.engine.Raft,
			RegionID: h.regionId,
			StartIdx: h.LastCompactedIdx,
			EndIdx: h.LastCompactedIdx,
		}
	}
}
func (h *proposalHandler) checkKeyInReq(req *raft_cmdpb.Request) error{
	//find the key
	var key []byte = nil
	switch req.CmdType {
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	}
	if key != nil {
		err := util.CheckKeyInRegion(key, h.Region())
		if err != nil {
			log.Infof(" %s key: %s not in region %d from %s to %s", h.Tag, string(key), h.regionId, string(h.Region().StartKey), string(h.Region().EndKey))
			return err
		}
	}
	return nil
}

func (h *proposalHandler) processNormalRequest(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry) {
	reqs := msg.Requests
	cb,ok := h.checkProposalCb(entry)
	if ok == false {
		log.Infof("Not ok.")
	}



	/*
	// if the epoch not match because of split, skip with error
	if msg.Header.RegionEpoch.Version != h.Region().RegionEpoch.Version {
		log.Infof(" %s Normal request process region not match.", h.Tag)
		if cb != nil {
			cb.Done(ErrResp(&util.ErrEpochNotMatch{
				Message: "Region epoch not match.",
				Regions: []*metapb.Region{h.Region()},
			}))
		}
		return
	}
	 */

	/*
	if err := h.checkKeyInRequests(reqs);err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	*/

	kvWb := new(engine_util.WriteBatch)
	resp := newCmdResp()
	for _,req := range reqs {
		//write command should be persisted first
		if err := h.checkKeyInReq(req); err != nil {
			if req.CmdType == raft_cmdpb.CmdType_Put || req.CmdType == raft_cmdpb.CmdType_Delete {
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				return
			}
			/*
			else if req.CmdType == raft_cmdpb.CmdType_Get || req.CmdType == raft_cmdpb.CmdType_Snap {
				continue
			}
			*/
		}
		kvWb.Reset()
		if ok {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Put:
				kvWb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
				h.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value) + len(req.Put.Cf))
			case raft_cmdpb.CmdType_Delete:
				kvWb.DeleteCF(req.Delete.Cf, req.Delete.Key)
				h.SizeDiffHint -= uint64(len(req.Delete.Cf) + len(req.Delete.Key))
			}
			kvWb.WriteToDB(h.peerStorage.Engines.Kv)
		}
		if cb != nil {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				log.Infof(" %s process Msg get, key: %v", h.Tag, req.Get.Key)
				//h.peerStorage.applyState.AppliedIndex = entry.Index
				//kvWb.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
				val,_ := engine_util.GetCF(h.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{Value: val},
				})
			case raft_cmdpb.CmdType_Delete:
				log.Infof(" %s process Msg delete, key: %s", h.Tag, string (req.Delete.Key))
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteResponse{},
				})
			case raft_cmdpb.CmdType_Put:
				log.Infof(" %s process Msg put, put key %s data: %s", h.Tag, string(req.Put.Key), string(req.Put.Value))
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put: &raft_cmdpb.PutResponse{},
				})
			case raft_cmdpb.CmdType_Snap:
				log.Infof(" %s process Msg snap.", h.Tag)
				if msg.Header.RegionEpoch.Version != h.Region().RegionEpoch.Version {
					cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
					return
				}
				// h.peerStorage.applyState.AppliedIndex = entry.Index
				//log.Infof("Process apply index %d", entry.Index)
				//kvWb.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
				//kvWb.WriteToDB(h.peerStorage.Engines.Kv)
				cb.Txn = h.peerStorage.Engines.Kv.NewTransaction(false)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{Region: h.Region()},
				})
			}
			// kvWb.WriteToDB(h.peerStorage.Engines.Kv)
		}
	}
	if cb != nil {
		cb.Done(resp)
	}
	/*
	kvWB := new(engine_util.WriteBatch)
	h.peerStorage.applyState.AppliedIndex = h.Entries[len(h.Entries)-1].Index
	kvWB.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
	kvWB.WriteToDB(h.peerStorage.Engines.Kv)
	 */
}

func (h *proposalHandler) bindError(err error, cb *message.Callback, resp *raft_cmdpb.RaftCmdResponse) {
	BindRespError(resp, err)
	if cb != nil {
		cb.Done(resp)
	}
}

func (h *proposalHandler) notifyStale(entry pb.Entry) {
	cb,_ := h.checkProposalCb(&entry)
	if cb != nil {
		NotifyStaleReq(h.Term(), cb)
	}
}

//check the proposal is valid or not, if valid return the callback
func (h *proposalHandler) checkProposalCb(entry *pb.Entry) (*message.Callback,bool) {
	for {
		if len(h.proposals) == 0 {
			log.Infof("%s no proposal.", h.Tag)
			return nil, true
		}
		proposal := h.proposals[0]

		if proposal.index < entry.Index {
			h.proposals = h.proposals[1:]
			proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		} else if proposal.index == entry.Index {
			h.proposals = h.proposals[1:]
			if proposal.term == entry.Term {
				log.Infof("find proposal, index:%d", proposal.index)
				return proposal.cb,true
			}
			// if the term not equal, means the entry may be stale
			NotifyStaleReq(entry.Term, proposal.cb)
			return nil, false
		} else {
			// proposal not found
			log.Infof("proposal not found", proposal.index)
			break
		}
	}
	return nil,true
}
