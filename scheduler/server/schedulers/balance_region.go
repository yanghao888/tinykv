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
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"go.uber.org/zap"
	"sort"
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
	stores := make(storeSlice, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if stores.Len() < 2 {
		return nil
	}
	sort.Sort(stores)

	var region *core.RegionInfo
	var fromStore, toStore *core.StoreInfo
	for i := stores.Len() - 1; i >= 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(container core.RegionsContainer) { regions = container })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(container core.RegionsContainer) { regions = container })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(container core.RegionsContainer) { regions = container })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
	}
	if region == nil {
		return nil
	}
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}

	for i, size := 0, len(stores); i < size; i++ {
		if _, ok := storeIds[stores[i].GetID()]; !ok {
			toStore = stores[i]
			break
		}
	}
	if toStore == nil || fromStore.GetRegionSize()-toStore.GetRegionSize() < region.GetApproximateSize()<<1 {
		return nil
	}

	peer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		return nil
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.GetId())
	if err != nil {
		log.Error("failed to create move peer operator", zap.Error(err))
		return nil
	}
	return op
}

type storeSlice []*core.StoreInfo

func (s storeSlice) Len() int           { return len(s) }
func (s storeSlice) Less(i, j int) bool { return s[i].GetRegionSize() < s[j].GetRegionSize() }
func (s storeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
