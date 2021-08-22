// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
	"github.com/pingcap-incubator/tinykv/log"
	"time"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	sources := cluster.GetStores()
	sort.Slice(sources, func(i,j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	targets := cluster.GetStores()
	sort.Slice(targets, func(i,j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})

	for i := 0; i < len(sources); i++ {
		source := sources[i]
		// Check if the source is available
		if !source.IsAvailable() {
			continue
		}
		// If the source is not up, pick next
		if !source.IsUp() {
			continue
		}
		// Check if the time last from lastHeartbeat above MinInterval
		if time.Now().Sub(source.GetLastHeartbeatTS()).Nanoseconds() > s.GetMinInterval().Nanoseconds() {
			continue
		}
		// A suitable store's down time cannot be longer than MaxStoreDownTIme of the cluster
		if source.DownTime() > cluster.GetMaxStoreDownTime() {
			return nil
		}
		// Try to pick target store
		for j := 0; j < balanceRegionRetryLimit; j++ {
			if op := s.transferRegionOut(cluster, source, targets); op != nil {
				return op
			}
		}
	}
	return nil
}

// transferRegionOut choose a proper region to transfer
func (s *balanceRegionScheduler) transferRegionOut(cluster opt.Cluster, source *core.StoreInfo, targets []*core.StoreInfo) *operator.Operator {
	var chosenRegion *core.RegionInfo
	// Get pending region first
	cluster.GetPendingRegionsWithLock(source.GetID(), func(regionContainer core.RegionsContainer) {
		// Means choose a random region from all regions in container
		chosenRegion = regionContainer.RandomRegion([]byte(""), []byte(""))
	})
	if op := s.checkTargetOut(cluster, source, targets, chosenRegion); op != nil {
		return op
	}

	// Get the follower region
	cluster.GetFollowersWithLock(source.GetID(), func(regionContainer core.RegionsContainer) {
		chosenRegion = regionContainer.RandomRegion([]byte(""), []byte(""))
	})
	if op := s.checkTargetOut(cluster, source, targets, chosenRegion); op != nil {
		return op
	}

	//Get the leader region
	cluster.GetLeadersWithLock(source.GetID(), func(regionContainer core.RegionsContainer) {
		chosenRegion = regionContainer.RandomRegion([]byte(""), []byte(""))
	})
	if op := s.checkTargetOut(cluster, source, targets, chosenRegion); op != nil {
		return op
	}
	return nil
}

// checkTargetOut find a target store to transfer after transferRegionOut get a proper region
func (s *balanceRegionScheduler) checkTargetOut(cluster opt.Cluster, source *core.StoreInfo, targets []*core.StoreInfo, chosenRegion *core.RegionInfo) *operator.Operator {
	allowPending := core.HealthRegionAllowPending()
	// If get pending region successfully and pending allowed, replicas qualified
	if chosenRegion != nil && allowPending(chosenRegion) && len(chosenRegion.GetPeers()) == cluster.GetMaxReplicas(){
		// log.Infof("choose region: %d",chosenRegion.GetID())
		filterTargets := s.storesFilterByRegion(chosenRegion, targets)
		for _,target := range filterTargets {
			// If the target size can't satisfy the size limit, other bigger target obviously can't satisfy, skip all
			if source.GetRegionSize() - target.GetRegionSize() < 2*chosenRegion.GetApproximateSize() {
				break
			}
			// If the target is not qualified, check others
			if !target.IsAvailable() || !target.IsUp() || target.DownTime() > cluster.GetMaxStoreDownTime() {
				continue
			}
 			if op := s.createOperator(cluster, chosenRegion, source, target); op != nil {
				// log.Infof("choose target successfully target store: %d", target.GetID())
				return op
			}
		}
	}
	return nil
}

// storesFilterByRegion filter the stores which contains the region
func (s *balanceRegionScheduler) storesFilterByRegion(region *core.RegionInfo, stores []*core.StoreInfo) []*core.StoreInfo {
	var filterStores []*core.StoreInfo
	for _,store := range stores {
		// Find the store without peer in this region
		if region.GetStorePeer(store.GetID()) == nil {
			filterStores = append(filterStores, store)
		}
	}
	return filterStores
}

// createOperator create operator to balance the region from store source to store target
func (s *balanceRegionScheduler) createOperator(cluster opt.Cluster, region *core.RegionInfo, source, target *core.StoreInfo) *operator.Operator{
	peer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		log.Errorf("Alloc peer failed in createOperator")
		return nil
	}
	op, err := operator.CreateMovePeerOperator("Move Peer", cluster, region, operator.OpBalance, source.GetID(), target.GetID(), peer.GetId())
	if err != nil {
		log.Errorf("Create operator failed in createOperator")
		return nil
	}
	return op
}

