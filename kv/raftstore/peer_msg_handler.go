package raftstore

import (
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

//contain a peer, use for filling into confchange msg
type peerContext struct {
	*metapb.Peer
}

func (pc *peerContext) Marshal() ([]byte,error){
	data := make([]byte, pc.Peer.Size())
	_,err := pc.Peer.MarshalTo(data)
	return data,err
}

func (pc *peerContext) Unmarshal(data []byte) error {
	var peer metapb.Peer
	err := peer.Unmarshal(data)
	if err != nil {
		return err
	}
	pc.Peer = &peer
	return nil
}


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
	if !d.RaftGroup.HasReady() {
		//log.Infof(" %s is not ready.", d.Tag)
		return
	}

	rd := d.RaftGroup.Ready()
	if len(rd.CommittedEntries) != 0 {
		log.Infof(" %s handle raft ready. commit msg first last commit index %d %d , last index %d", d.Tag, rd.CommittedEntries[0].Index,rd.CommittedEntries[len(rd.CommittedEntries)-1].Index, d.RaftGroup.Raft.RaftLog.LastIndex())
	} else {
		log.Infof(" %s handle raft ready. No commit entry", d.Tag)
	}
	applySnapRes,err := d.peerStorage.SaveReadyState(&rd)

	if err != nil {
		panic(err)
	}

	if applySnapRes != nil {
		if !util.RegionEqual(applySnapRes.PrevRegion, applySnapRes.Region) {
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.setRegion(applySnapRes.Region, d.peer)
			d.ctx.storeMeta.regions[applySnapRes.Region.Id] = applySnapRes.Region
			if len(applySnapRes.PrevRegion.Peers) != 0 {
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapRes.PrevRegion})
			}
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapRes.Region})
			d.ctx.storeMeta.Unlock()
		}
	}
	log.Infof("raft ready messages len %d", len(rd.Messages))
	d.Send(d.ctx.trans, rd.Messages)
	proposalHandler := newProposalHandler(d, rd.CommittedEntries)
	proposalHandler.HandleProposal()
	// d.proposals = nil
	d.RaftGroup.Advance(rd)

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {

	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
//		log.Infof(" %s handle raft message %s from %d to %d, MsgType: %v" ,
//				d.Tag, raftMsg.GetMessage().GetMsgType(), raftMsg.GetFromPeer().GetId(), raftMsg.GetToPeer().GetId(),raftMsg.Message.MsgType)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		log.Infof(" %s handle raft command, peer %d store %d cmdType: %s.", d.Tag, raftCMD.Request.Header.Peer.Id, raftCMD.Request.Header.Peer.StoreId, util.GetRequestType(raftCMD.Request))
		if raftCMD.Request != nil && raftCMD.Request.Requests != nil{
			for _,req := range raftCMD.Request.Requests {
				switch req.CmdType {
				case raft_cmdpb.CmdType_Put:
					log.Infof("%s propose msg type put key %s", d.Tag, string(req.Put.Key))
				case raft_cmdpb.CmdType_Delete:
					log.Infof("%s propose msg type delete key %s", d.Tag, string(req.Delete.Key))
				}

			}
		}
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		log.Infof(" %s Tick message.\n",d.Tag)
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		log.Infof(" %s RaftCmd: %v Peerid: %v\n",d.Tag, d.Meta.Id, d.Meta.Id)
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		log.Infof(" %s Peer id: %d Message Start.\n",d.Tag, d.Meta.Id)
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		log.Infof("store not match")
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		log.Infof("not the leader")
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		log.Infof("not the peer")
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		log.Infof("not the term")
		return err
	}
//	log.Infof("Req region ver %d cf ver %d curRegion ver %d cf ver %d", req.Header.RegionEpoch.Version, req.Header.RegionEpoch.ConfVer, d.Region().RegionEpoch.Version, d.Region().RegionEpoch.ConfVer)
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is   not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			log.Infof(" %s find siblingRegion id %d, start key %s, end key %s", d.Tag, siblingRegion.Id, string(siblingRegion.StartKey), string(siblingRegion.EndKey))

			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		// log.Infof(" %s not find siblingRegion", d.Tag)
		return errEpochNotMatching
	}
	return err
}


//propose the normal request in the RaftCmdRequest
func (d *peerMsgHandler) proposeNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	reqs := msg.Requests

	//check the key in reqs
	if err := d.checkKeyInRequests(reqs);err != nil{
		// log.Infof("not in requests")
		cb.Done(ErrResp(err))
		return

	}

	//check if the read and write only has one
	if err := d.checkSingleOption(reqs);err != nil {
		panic(err)
	}

	data,err := msg.Marshal()
	if err != nil {
		panic(err)
	}


	pIndex := d.nextProposalIndex()
	pTerm := d.Term()
	// log.Infof("Propose normal request")
	if err := d.RaftGroup.Propose(data); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	d.proposals = append(d.proposals, &proposal{
		index: pIndex,
		term: pTerm,
		cb: cb,
	})
	log.Infof("proposal term index %v %v", d.nextProposalIndex(),d.Term())
}


// proposeCompactLog propose the admin request Compact Log
func (d *peerMsgHandler) proposeCompactLog(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// just propose the message as a entry
	data,err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	if err = d.RaftGroup.Propose(data);err != nil {
		cb.Done(ErrResp(err))
	}
	return
}

// proposeTransferLeader propose the admin request TransferLeader
func (d *peerMsgHandler) proposeTransferLeader(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	transferLeaderRequest := msg.GetAdminRequest().GetTransferLeader()
	d.RaftGroup.TransferLeader(transferLeaderRequest.GetPeer().GetId())
	if cb != nil {
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader:&raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
	}
	return
}

// proposeChangePeer
func (d *peerMsgHandler) proposeChangePeer(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	changePeerRequest := msg.GetAdminRequest().GetChangePeer()

	//drive the raft propose Conf change
	context,_ := msg.Marshal()


	confChange := pb.ConfChange{
		ChangeType: changePeerRequest.GetChangeType(),
		NodeId: changePeerRequest.GetPeer().GetId(),
		Context: context,
	}

	pIndex := d.nextProposalIndex()
	pTerm := d.Term()

	if err :=d.RaftGroup.ProposeConfChange(confChange); err != nil {
		cb.Done(ErrResp(err))
	} else {
		d.proposals = append(d.proposals, &proposal{
			index: pIndex,
			term: pTerm,
			cb: cb,
		})
	}
}

func (d *peerMsgHandler) proposeSplit(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
	if err != nil {
		cb.Done(ErrResp(err))
	}

	data,err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
	}


	pIndex := d.nextProposalIndex()
	pTerm := d.Term()
	// log.Infof("Propose normal request")
	if err := d.RaftGroup.Propose(data); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	d.proposals = append(d.proposals, &proposal{
		index: pIndex,
		term: pTerm,
		cb: cb,
	})


}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.GetAdminRequest().GetCmdType() {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.proposeCompactLog(msg, cb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.proposeTransferLeader(msg, cb)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.proposeChangePeer(msg, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.proposeSplit(msg,cb)
	}


}



func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		log.Infof("pre failed")
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	switch {
	case msg.GetAdminRequest() != nil:

		d.proposeAdminRequest(msg,cb)
	case len(msg.Requests) > 0 :

		d.proposeNormalRequest(msg,cb)
	}

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
	log.Debugf ("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		log.Infof("Raft Message %s not valid.",msg.GetMessage().GetMsgType())
		return nil
	}
	if d.stopped {
		log.Infof("Raft Message %s stop.",msg.GetMessage().GetMsgType())
		return nil
	}
	if msg.GetIsTombstone() {
		log.Infof("Raft Message %s Tombstone.",msg.GetMessage().GetMsgType())
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		log.Infof("Raft Message %s Tombstone.",msg.GetMessage().GetMsgType())
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
		log.Infof("Raft Message %s key is nil.",msg.GetMessage().GetMsgType())
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
		//fmt.Printf("msg type %v", msg.Message.MsgType)
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

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
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

// check the key in requests
func (d *peerMsgHandler) checkKeyInRequests(reqs []*raft_cmdpb.Request ) error {
	for _,req := range reqs {
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
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				return err
			}
		}

	}
	return nil
}

//check if the write or read only has one
func (d *peerMsgHandler) checkSingleOption(reqs []*raft_cmdpb.Request ) error {
	readFlag,writeFlag := false, false
	for _,req := range reqs {
		if req.CmdType == raft_cmdpb.CmdType_Get {
			readFlag = true
		} else {
			writeFlag = true
		}
	}
	if writeFlag && readFlag {
		err := errors.Errorf("Write and read command should not in one requests")
		log.Error(err)
		return err
	}
	return nil
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
	log.Infof(" %s is Cur leader, store %d, peers: %v", d.Tag, d.storeID(), d.Region().Peers)
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

func (d *peerMsgHandler) setRegionWithLock (region *metapb.Region) {
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: d.Region()})
	d.ctx.storeMeta.setRegion(region, d.peer)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	d.ctx.storeMeta.Unlock()
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


