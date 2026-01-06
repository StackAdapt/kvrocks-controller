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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/apache/kvrocks-controller/store"
)

const totalSlots = 16384

type Migration struct {
	SourceShard int             `json:"source_shard"`
	TargetShard int             `json:"target_shard"`
	Slots       store.SlotRange `json:"slots"`
}

type MigrationBatch struct {
	Batch      int         `json:"batch"`
	Concurrent bool        `json:"concurrent"`
	Migrations []Migration `json:"migrations"`
}

type BalancePlan struct {
	CurrentDistribution []int            `json:"current_distribution"`
	TargetDistribution  []int            `json:"target_distribution"`
	TotalSlotsToMove    int              `json:"total_slots_to_move"`
	Batches             []MigrationBatch `json:"batches"`
}

type shardSlotInfo struct {
	index     int
	slotCount int
	delta     int // positive = needs to give away, negative = needs to receive
}

func main() {
	inputFile := flag.String("input", "", "Path to cluster JSON file (reads from stdin if not provided)")
	outputJSON := flag.Bool("json", false, "Output as JSON instead of commands")
	namespace := flag.String("namespace", "default", "Namespace for kvctl commands")
	cluster := flag.String("cluster", "", "Cluster name for kvctl commands")
	flag.Parse()

	var data []byte
	var err error

	if *inputFile != "" {
		data, err = os.ReadFile(*inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
			os.Exit(1)
		}
	} else {
		data, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
			os.Exit(1)
		}
	}

	var clusterData store.Cluster
	if err := json.Unmarshal(data, &clusterData); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing cluster JSON: %v\n", err)
		os.Exit(1)
	}

	plan := calculateBalancePlan(&clusterData)

	if *outputJSON {
		output, _ := json.MarshalIndent(plan, "", "  ")
		fmt.Println(string(output))
	} else {
		printCommands(plan, *namespace, *cluster)
	}
}

func countSlots(slotRanges []store.SlotRange) int {
	count := 0
	for _, sr := range slotRanges {
		count += sr.Stop - sr.Start + 1
	}
	return count
}

func calculateBalancePlan(cluster *store.Cluster) *BalancePlan {
	numShards := len(cluster.Shards)
	if numShards == 0 {
		return &BalancePlan{}
	}

	// Calculate current distribution
	currentDist := make([]int, numShards)
	for i, shard := range cluster.Shards {
		currentDist[i] = countSlots(shard.SlotRanges)
	}

	// Calculate target distribution (balanced)
	baseSlots := totalSlots / numShards
	remainder := totalSlots % numShards
	targetDist := make([]int, numShards)
	for i := range targetDist {
		targetDist[i] = baseSlots
		if i < remainder {
			targetDist[i]++
		}
	}

	// Calculate deltas: positive = overage (give away), negative = shortage (receive)
	shardInfos := make([]shardSlotInfo, numShards)
	for i := range shardInfos {
		shardInfos[i] = shardSlotInfo{
			index:     i,
			slotCount: currentDist[i],
			delta:     currentDist[i] - targetDist[i],
		}
	}

	// Generate migrations
	migrations := generateMigrations(cluster, shardInfos)

	// Batch migrations for concurrency
	batches := batchMigrations(migrations)

	// Calculate total slots to move
	totalToMove := 0
	for _, m := range migrations {
		totalToMove += m.Slots.Stop - m.Slots.Start + 1
	}

	return &BalancePlan{
		CurrentDistribution: currentDist,
		TargetDistribution:  targetDist,
		TotalSlotsToMove:    totalToMove,
		Batches:             batches,
	}
}

func generateMigrations(cluster *store.Cluster, shardInfos []shardSlotInfo) []Migration {
	var migrations []Migration

	// Separate into givers (positive delta) and receivers (negative delta)
	var givers, receivers []shardSlotInfo
	for _, info := range shardInfos {
		if info.delta > 0 {
			givers = append(givers, info)
		} else if info.delta < 0 {
			receivers = append(receivers, info)
		}
	}

	// Sort givers by delta descending (most overage first)
	sort.Slice(givers, func(i, j int) bool {
		return givers[i].delta > givers[j].delta
	})

	// Sort receivers by delta ascending (most shortage first)
	sort.Slice(receivers, func(i, j int) bool {
		return receivers[i].delta < receivers[j].delta
	})

	// Track how many slots each shard still needs to give/receive
	giverRemaining := make(map[int]int)
	receiverRemaining := make(map[int]int)
	for _, g := range givers {
		giverRemaining[g.index] = g.delta
	}
	for _, r := range receivers {
		receiverRemaining[r.index] = -r.delta // make positive
	}

	// Track current slot position for each giver shard
	giverSlotPos := make(map[int]int) // tracks which slot to start giving from
	for _, g := range givers {
		if len(cluster.Shards[g.index].SlotRanges) > 0 {
			// Start from the end of their slot ranges
			lastRange := cluster.Shards[g.index].SlotRanges[len(cluster.Shards[g.index].SlotRanges)-1]
			giverSlotPos[g.index] = lastRange.Stop
		}
	}

	// Greedy matching: pair givers with receivers
	for _, giver := range givers {
		giverIdx := giver.index
		shard := cluster.Shards[giverIdx]

		// Build a list of all slots this shard owns (in reverse order, we take from the end)
		var allSlots []int
		for _, sr := range shard.SlotRanges {
			for s := sr.Start; s <= sr.Stop; s++ {
				allSlots = append(allSlots, s)
			}
		}
		// Reverse to take from end
		for i, j := 0, len(allSlots)-1; i < j; i, j = i+1, j-1 {
			allSlots[i], allSlots[j] = allSlots[j], allSlots[i]
		}

		slotIdx := 0
		for _, receiver := range receivers {
			receiverIdx := receiver.index
			if giverRemaining[giverIdx] <= 0 {
				break
			}
			if receiverRemaining[receiverIdx] <= 0 {
				continue
			}

			// How many slots to transfer in this migration
			toTransfer := min(giverRemaining[giverIdx], receiverRemaining[receiverIdx])

			// Get the actual slots to transfer (as contiguous ranges where possible)
			slotsToTransfer := allSlots[slotIdx : slotIdx+toTransfer]
			slotIdx += toTransfer

			// Group into contiguous ranges
			ranges := groupIntoRanges(slotsToTransfer)
			for _, sr := range ranges {
				migrations = append(migrations, Migration{
					SourceShard: giverIdx,
					TargetShard: receiverIdx,
					Slots:       sr,
				})
			}

			giverRemaining[giverIdx] -= toTransfer
			receiverRemaining[receiverIdx] -= toTransfer
		}
	}

	return migrations
}

func groupIntoRanges(slots []int) []store.SlotRange {
	if len(slots) == 0 {
		return nil
	}

	// Sort slots
	sort.Ints(slots)

	var ranges []store.SlotRange
	start := slots[0]
	end := slots[0]

	for i := 1; i < len(slots); i++ {
		if slots[i] == end+1 {
			end = slots[i]
		} else {
			ranges = append(ranges, store.SlotRange{Start: start, Stop: end})
			start = slots[i]
			end = slots[i]
		}
	}
	ranges = append(ranges, store.SlotRange{Start: start, Stop: end})

	return ranges
}

func batchMigrations(migrations []Migration) []MigrationBatch {
	if len(migrations) == 0 {
		return nil
	}

	var batches []MigrationBatch
	remaining := make([]Migration, len(migrations))
	copy(remaining, migrations)

	batchNum := 1
	for len(remaining) > 0 {
		busyShards := make(map[int]bool)
		var batch []Migration
		var next []Migration

		for _, m := range remaining {
			// Check if source or target is already busy in this batch
			if busyShards[m.SourceShard] || busyShards[m.TargetShard] {
				next = append(next, m)
				continue
			}
			// Add to current batch
			batch = append(batch, m)
			busyShards[m.SourceShard] = true
			busyShards[m.TargetShard] = true
		}

		batches = append(batches, MigrationBatch{
			Batch:      batchNum,
			Concurrent: len(batch) > 1,
			Migrations: batch,
		})
		batchNum++
		remaining = next
	}

	return batches
}

func printCommands(plan *BalancePlan, namespace, cluster string) {
	fmt.Println("# Cluster Balance Plan")
	fmt.Println("#")
	fmt.Printf("# Current distribution: %v\n", plan.CurrentDistribution)
	fmt.Printf("# Target distribution:  %v\n", plan.TargetDistribution)
	fmt.Printf("# Total slots to move:  %d\n", plan.TotalSlotsToMove)
	fmt.Println()

	if len(plan.Batches) == 0 {
		fmt.Println("# Cluster is already balanced!")
		return
	}

	for _, batch := range plan.Batches {
		if batch.Concurrent {
			fmt.Printf("# === Batch %d (run these %d migrations concurrently) ===\n", batch.Batch, len(batch.Migrations))
		} else {
			fmt.Printf("# === Batch %d ===\n", batch.Batch)
		}

		for _, m := range batch.Migrations {
			slotArg := m.Slots.String()
			fmt.Printf("kvctl migrate slot %s --target %d -n %s -c %s  # shard %d -> %d\n",
				slotArg, m.TargetShard, namespace, cluster, m.SourceShard, m.TargetShard)
		}
		fmt.Println()
	}

	fmt.Println("# Wait for each batch to complete before starting the next batch.")
	fmt.Println("# Migrations within the same batch can run concurrently.")
}
