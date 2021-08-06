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
			return
		case entry.EntryType == pb.EntryType_EntryConfChange:
			h.processConfChange(entry)
		case msg.AdminRequest != nil:
			h.processAdminRequest(msg, entry)
		case len(msg.Requests) > 0:
			h.processNormalRequest(msg, &entry)
		}
	}
	return
}

// processAddNode process the add node command in ConfChange
func (h *proposalHandler) processAddNode(confChange pb.ConfChange) {
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
	h.ctx.storeMeta.Lock()
	h.ctx.storeMeta.regionRanges.Delete(&regionItem{region: h.Region()})
	h.ctx.storeMeta.setRegion(region, h.peer)
	h.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	h.ctx.storeMeta.Unlock()
	// log.Infof
	log.Infof("%s region: %d, add Peer %d in peerCache.\n", h.Tag, h.regionId, changePeer.Id)
	fmt.Printf("%s region: %d, add Peer %d in peerCache.\n", h.Tag, h.regionId, changePeer.Id)
}

// processRemoveNode process the remove node command in ConfChange
func (h *proposalHandler) processRemoveNode(confChange pb.ConfChange) {
	region := &metapb.Region{}
	util.CloneMsg(h.Region(), region)

	reqCMD := &raft_cmdpb.RaftCmdRequest{}
	if err := reqCMD.Unmarshal(confChange.Context); err != nil {
		panic(err)
	}

	// peer has already removed
	removePeer := reqCMD.GetAdminRequest().GetChangePeer().GetPeer()
	if util.RemovePeer(region, removePeer.GetStoreId()) == nil {
		fmt.Printf("peer is already removed!\n")
		return
	}

	region.RegionEpoch.ConfVer++

	// remove itself
	if h.Meta.GetId() == confChange.NodeId {
		// refresh the region
		h.ctx.storeMeta.Lock()
		h.ctx.storeMeta.regionRanges.Delete(&regionItem{region: h.Region()})
		h.ctx.storeMeta.setRegion(region, h.peer)
		h.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		h.ctx.storeMeta.Unlock()
		h.destroyPeer()
		//log.Infof
		fmt.Printf("%s region %d peer %d remove itself already.\n", h.Tag, region.Id, removePeer.Id)
		return
	}
	kvWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	if err := kvWB.WriteToDB(h.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}
	h.removePeerCache(removePeer.Id)
	h.ctx.storeMeta.Lock()
	h.ctx.storeMeta.regionRanges.Delete(&regionItem{region: h.Region()})
	h.ctx.storeMeta.setRegion(region, h.peer)
	h.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	h.ctx.storeMeta.Unlock()
	log.Infof("%s region %d peer %d remove already.\n", h.Tag, region.Id, removePeer.Id)
	fmt.Printf("%s region %d peer %d remove already.\n", h.Tag, region.Id, removePeer.Id)
	return
}

// processConfChange process the conf change entry
func (h *proposalHandler) processConfChange(entry pb.Entry) {
	confChange := pb.ConfChange{}
	if err := confChange.Unmarshal(entry.GetData()); err != nil {
		panic(err)
	}


	switch confChange.ChangeType {
	case pb.ConfChangeType_AddNode:
		h.processAddNode(confChange)
	case pb.ConfChangeType_RemoveNode:
		h.processRemoveNode(confChange)
	}

	h.RaftGroup.ApplyConfChange(confChange)

	if h.IsLeader() {
		h.HeartbeatScheduler(h.ctx.schedulerTaskSender)
	}
}



func (h *proposalHandler) processAdminRequest(msg *raft_cmdpb.RaftCmdRequest, entry pb.Entry) {
	log.Infof("[Region: %d] %d store: %d apply admin msg: %v",msg.Header.RegionId,msg.Header.Peer.Id, msg.Header.Peer.StoreId, msg.AdminRequest.CmdType)
	adReq := msg.AdminRequest
	//resp := newCmdResp()
	//cb,ok := h.checkProposalCb(entry)
	//if ok == false {
		//return
	//}
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		h.processCompactLog(adReq)
	case raft_cmdpb.AdminCmdType_Split:
		h.processSplit(msg,entry)
	}
}

func (h *proposalHandler) processSplit(msg *raft_cmdpb.RaftCmdRequest, entry pb.Entry) {
	/*splitMsg := msg.AdminRequest.Split

	resp := newCmdResp()
	region := &metapb.Region{}
	if err := util.CloneMsg(h.Region(), region); err != nil {
		return
	}
	// If the split key is not in this region
	if err := util.CheckKeyInRegion(splitMsg.SplitKey, region); err != nil {
		//BindRespError()
	}

	 */
}

func(h *proposalHandler) processCompactLog(adReq *raft_cmdpb.AdminRequest) {
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

func (h *proposalHandler) processNormalRequest(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry) {
	reqs := msg.Requests
	cb,ok := h.checkProposalCb(entry)

	if err := h.checkKeyInRequests(reqs);err != nil {
		if ok == true {
			cb.Done(ErrResp(err))
		}
		return
	}

	kvWb := new(engine_util.WriteBatch)
	resp := newCmdResp()
	for _,req := range reqs {
		//write command should be persisted first
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			kvWb.DeleteCF(req.Delete.Cf, req.Delete.Key)
		}
		kvWb.WriteToDB(h.peerStorage.Engines.Kv)
		kvWb.Reset()
		if ok == true {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				log.Infof(" %s process Msg get, key: %v", h.Tag, req.Get.Key)
				h.peerStorage.applyState.AppliedIndex = entry.Index
				kvWb.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
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
				h.peerStorage.applyState.AppliedIndex = entry.Index
				kvWb.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
				kvWb.WriteToDB(h.peerStorage.Engines.Kv)
				cb.Txn = h.peerStorage.Engines.Kv.NewTransaction(false)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{Region: h.Region()},
				})
			}
			kvWb.WriteToDB(h.peerStorage.Engines.Kv)
		}
	}
	if cb != nil {
		cb.Done(resp)
	}
	kvWB := new(engine_util.WriteBatch)
	h.peerStorage.applyState.AppliedIndex = h.Entries[len(h.Entries)-1].Index
	kvWB.SetMeta(meta.ApplyStateKey(h.regionId), h.peerStorage.applyState)
	kvWB.WriteToDB(h.peerStorage.Engines.Kv)
}

//check the proposal is valid or not, if valid return the callback
func (h *proposalHandler) checkProposalCb(entry *pb.Entry) (*message.Callback,bool) {
	for {
		if len(h.proposals) == 0 {
			log.Infof("%s no proposal.", h.Tag)
			return nil,false
		}
		proposal := h.proposals[0]
		h.proposals = h.proposals[1:]

		if proposal.index < entry.Index {
			proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		} else if proposal.index == entry.Index {

			if proposal.term == entry.Term {
				log.Infof("find proposal, index:%d", proposal.index)
				return proposal.cb,true
			}
			// if the term not equal, means the entry may be stale
			NotifyStaleReq(entry.Term, proposal.cb)
			return nil, false
		} else {
			// proposal not found
			break
		}
	}
	return nil,false
}
