/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package main

import (
	"testing"

	"github.com/apache/kvrocks-controller/store"
)

func TestCountSlots(t *testing.T) {
	tests := []struct {
		name       string
		slotRanges []store.SlotRange
		expected   int
	}{
		{
			name:       "empty",
			slotRanges: []store.SlotRange{},
			expected:   0,
		},
		{
			name:       "single slot",
			slotRanges: []store.SlotRange{{Start: 0, Stop: 0}},
			expected:   1,
		},
		{
			name:       "single range",
			slotRanges: []store.SlotRange{{Start: 0, Stop: 99}},
			expected:   100,
		},
		{
			name: "multiple ranges",
			slotRanges: []store.SlotRange{
				{Start: 0, Stop: 99},
				{Start: 200, Stop: 299},
			},
			expected: 200,
		},
		{
			name:       "full cluster single shard",
			slotRanges: []store.SlotRange{{Start: 0, Stop: 16383}},
			expected:   16384,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countSlots(tt.slotRanges)
			if result != tt.expected {
				t.Errorf("countSlots() = %d, expected %d", result, tt.expected)
			}
		})
	}
}

func TestGroupIntoRanges(t *testing.T) {
	tests := []struct {
		name     string
		slots    []int
		expected []store.SlotRange
	}{
		{
			name:     "empty",
			slots:    []int{},
			expected: nil,
		},
		{
			name:     "single slot",
			slots:    []int{5},
			expected: []store.SlotRange{{Start: 5, Stop: 5}},
		},
		{
			name:     "contiguous slots",
			slots:    []int{1, 2, 3, 4, 5},
			expected: []store.SlotRange{{Start: 1, Stop: 5}},
		},
		{
			name:     "non-contiguous slots",
			slots:    []int{1, 2, 3, 10, 11, 12},
			expected: []store.SlotRange{{Start: 1, Stop: 3}, {Start: 10, Stop: 12}},
		},
		{
			name:     "unordered slots",
			slots:    []int{5, 3, 4, 1, 2},
			expected: []store.SlotRange{{Start: 1, Stop: 5}},
		},
		{
			name:  "multiple gaps",
			slots: []int{1, 5, 6, 10},
			expected: []store.SlotRange{
				{Start: 1, Stop: 1},
				{Start: 5, Stop: 6},
				{Start: 10, Stop: 10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupIntoRanges(tt.slots)
			if len(result) != len(tt.expected) {
				t.Errorf("groupIntoRanges() returned %d ranges, expected %d", len(result), len(tt.expected))
				return
			}
			for i, r := range result {
				if r.Start != tt.expected[i].Start || r.Stop != tt.expected[i].Stop {
					t.Errorf("groupIntoRanges()[%d] = %v, expected %v", i, r, tt.expected[i])
				}
			}
		})
	}
}

func TestBatchMigrations(t *testing.T) {
	tests := []struct {
		name               string
		migrations         []Migration
		expectedBatchCount int
		expectedBatchSizes []int
	}{
		{
			name:               "empty",
			migrations:         []Migration{},
			expectedBatchCount: 0,
			expectedBatchSizes: nil,
		},
		{
			name: "single migration",
			migrations: []Migration{
				{SourceShard: 0, TargetShard: 1, Slots: store.SlotRange{Start: 0, Stop: 100}},
			},
			expectedBatchCount: 1,
			expectedBatchSizes: []int{1},
		},
		{
			name: "two independent migrations - can be concurrent",
			migrations: []Migration{
				{SourceShard: 0, TargetShard: 1, Slots: store.SlotRange{Start: 0, Stop: 100}},
				{SourceShard: 2, TargetShard: 3, Slots: store.SlotRange{Start: 200, Stop: 300}},
			},
			expectedBatchCount: 1,
			expectedBatchSizes: []int{2},
		},
		{
			name: "two dependent migrations - same source",
			migrations: []Migration{
				{SourceShard: 0, TargetShard: 1, Slots: store.SlotRange{Start: 0, Stop: 100}},
				{SourceShard: 0, TargetShard: 2, Slots: store.SlotRange{Start: 101, Stop: 200}},
			},
			expectedBatchCount: 2,
			expectedBatchSizes: []int{1, 1},
		},
		{
			name: "two dependent migrations - same target",
			migrations: []Migration{
				{SourceShard: 0, TargetShard: 2, Slots: store.SlotRange{Start: 0, Stop: 100}},
				{SourceShard: 1, TargetShard: 2, Slots: store.SlotRange{Start: 200, Stop: 300}},
			},
			expectedBatchCount: 2,
			expectedBatchSizes: []int{1, 1},
		},
		{
			name: "complex scenario - A,B,C to D,E",
			migrations: []Migration{
				{SourceShard: 0, TargetShard: 3, Slots: store.SlotRange{Start: 0, Stop: 100}},   // A -> D
				{SourceShard: 2, TargetShard: 4, Slots: store.SlotRange{Start: 200, Stop: 300}}, // C -> E
				{SourceShard: 1, TargetShard: 3, Slots: store.SlotRange{Start: 400, Stop: 500}}, // B -> D
				{SourceShard: 1, TargetShard: 4, Slots: store.SlotRange{Start: 600, Stop: 700}}, // B -> E
			},
			expectedBatchCount: 3,
			expectedBatchSizes: []int{2, 1, 1}, // (A->D, C->E), (B->D), (B->E)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := batchMigrations(tt.migrations)
			if len(result) != tt.expectedBatchCount {
				t.Errorf("batchMigrations() returned %d batches, expected %d", len(result), tt.expectedBatchCount)
				return
			}
			for i, batch := range result {
				if len(batch.Migrations) != tt.expectedBatchSizes[i] {
					t.Errorf("batch[%d] has %d migrations, expected %d", i, len(batch.Migrations), tt.expectedBatchSizes[i])
				}
			}
		})
	}
}

func TestBatchMigrationsNoConcurrentConflicts(t *testing.T) {
	// Test that no batch has conflicting shards
	migrations := []Migration{
		{SourceShard: 0, TargetShard: 3, Slots: store.SlotRange{Start: 0, Stop: 100}},
		{SourceShard: 1, TargetShard: 4, Slots: store.SlotRange{Start: 200, Stop: 300}},
		{SourceShard: 2, TargetShard: 5, Slots: store.SlotRange{Start: 400, Stop: 500}},
		{SourceShard: 0, TargetShard: 4, Slots: store.SlotRange{Start: 600, Stop: 700}},
		{SourceShard: 1, TargetShard: 5, Slots: store.SlotRange{Start: 800, Stop: 900}},
	}

	batches := batchMigrations(migrations)

	for batchIdx, batch := range batches {
		usedShards := make(map[int]bool)
		for _, m := range batch.Migrations {
			if usedShards[m.SourceShard] {
				t.Errorf("batch[%d] has duplicate source shard %d", batchIdx, m.SourceShard)
			}
			if usedShards[m.TargetShard] {
				t.Errorf("batch[%d] has duplicate target shard %d", batchIdx, m.TargetShard)
			}
			if m.SourceShard == m.TargetShard {
				t.Errorf("batch[%d] has migration with same source and target %d", batchIdx, m.SourceShard)
			}
			usedShards[m.SourceShard] = true
			usedShards[m.TargetShard] = true
		}
	}
}

func TestCalculateBalancePlan(t *testing.T) {
	tests := []struct {
		name                   string
		cluster                *store.Cluster
		expectedCurrentDist    []int
		expectedTargetDist     []int
		expectedTotalSlotsMove int
	}{
		{
			name:                   "empty cluster",
			cluster:                &store.Cluster{Shards: []*store.Shard{}},
			expectedCurrentDist:    []int{},
			expectedTargetDist:     []int{},
			expectedTotalSlotsMove: 0,
		},
		{
			name: "already balanced - 2 shards",
			cluster: &store.Cluster{
				Shards: []*store.Shard{
					{SlotRanges: []store.SlotRange{{Start: 0, Stop: 8191}}},
					{SlotRanges: []store.SlotRange{{Start: 8192, Stop: 16383}}},
				},
			},
			expectedCurrentDist:    []int{8192, 8192},
			expectedTargetDist:     []int{8192, 8192},
			expectedTotalSlotsMove: 0,
		},
		{
			name: "add one empty shard - 3 shards total",
			cluster: &store.Cluster{
				Shards: []*store.Shard{
					{SlotRanges: []store.SlotRange{{Start: 0, Stop: 8191}}},
					{SlotRanges: []store.SlotRange{{Start: 8192, Stop: 16383}}},
					{SlotRanges: []store.SlotRange{}},
				},
			},
			expectedCurrentDist: []int{8192, 8192, 0},
			expectedTargetDist:  []int{5462, 5461, 5461}, // 16384 / 3 = 5461 r 1
		},
		{
			name: "add two empty shards - 4 shards total",
			cluster: &store.Cluster{
				Shards: []*store.Shard{
					{SlotRanges: []store.SlotRange{{Start: 0, Stop: 8191}}},
					{SlotRanges: []store.SlotRange{{Start: 8192, Stop: 16383}}},
					{SlotRanges: []store.SlotRange{}},
					{SlotRanges: []store.SlotRange{}},
				},
			},
			expectedCurrentDist: []int{8192, 8192, 0, 0},
			expectedTargetDist:  []int{4096, 4096, 4096, 4096},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := calculateBalancePlan(tt.cluster)

			if len(plan.CurrentDistribution) != len(tt.expectedCurrentDist) {
				t.Errorf("CurrentDistribution length = %d, expected %d",
					len(plan.CurrentDistribution), len(tt.expectedCurrentDist))
				return
			}

			for i, v := range plan.CurrentDistribution {
				if v != tt.expectedCurrentDist[i] {
					t.Errorf("CurrentDistribution[%d] = %d, expected %d", i, v, tt.expectedCurrentDist[i])
				}
			}

			for i, v := range plan.TargetDistribution {
				if v != tt.expectedTargetDist[i] {
					t.Errorf("TargetDistribution[%d] = %d, expected %d", i, v, tt.expectedTargetDist[i])
				}
			}

			// Verify total slots moved matches what migrations would move
			totalFromMigrations := 0
			for _, batch := range plan.Batches {
				for _, m := range batch.Migrations {
					totalFromMigrations += m.Slots.Stop - m.Slots.Start + 1
				}
			}
			if totalFromMigrations != plan.TotalSlotsToMove {
				t.Errorf("TotalSlotsToMove = %d, but migrations total = %d",
					plan.TotalSlotsToMove, totalFromMigrations)
			}
		})
	}
}

func TestCalculateBalancePlanPreservesAllSlots(t *testing.T) {
	// Test that after applying migrations, all slots are accounted for
	cluster := &store.Cluster{
		Shards: []*store.Shard{
			{SlotRanges: []store.SlotRange{{Start: 0, Stop: 5460}}},
			{SlotRanges: []store.SlotRange{{Start: 5461, Stop: 10921}}},
			{SlotRanges: []store.SlotRange{{Start: 10922, Stop: 16383}}},
			{SlotRanges: []store.SlotRange{}}, // new empty shard
			{SlotRanges: []store.SlotRange{}}, // new empty shard
		},
	}

	plan := calculateBalancePlan(cluster)

	// Calculate final distribution after migrations
	finalDist := make([]int, len(cluster.Shards))
	copy(finalDist, plan.CurrentDistribution)

	for _, batch := range plan.Batches {
		for _, m := range batch.Migrations {
			slots := m.Slots.Stop - m.Slots.Start + 1
			finalDist[m.SourceShard] -= slots
			finalDist[m.TargetShard] += slots
		}
	}

	// Verify final distribution matches target
	for i, v := range finalDist {
		if v != plan.TargetDistribution[i] {
			t.Errorf("Final distribution[%d] = %d, expected %d", i, v, plan.TargetDistribution[i])
		}
	}

	// Verify total slots is still 16384
	total := 0
	for _, v := range finalDist {
		total += v
	}
	if total != 16384 {
		t.Errorf("Total slots after migration = %d, expected 16384", total)
	}
}
