# Balance

A tool to generate optimized migration commands for rebalancing a kvrocks cluster.

## Usage

```sh
# From stdin
cat cluster.json | go run ./cmd/balance -namespace myns -cluster mycluster

# From file
go run ./cmd/balance -input cluster.json -namespace myns -cluster mycluster

# Output as JSON instead of kvctl commands
go run ./cmd/balance -input cluster.json -json
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-input` | (stdin) | Path to cluster JSON file |
| `-json` | false | Output as JSON instead of kvctl commands |
| `-namespace` | default | Namespace for kvctl commands |
| `-cluster` | (required) | Cluster name for kvctl commands |

## How It Works

1. **Calculates balanced distribution** - Divides 16384 slots evenly across all shards
2. **Minimizes operations** - Only moves slots from shards with excess to shards with deficit
3. **Batches for concurrency** - Groups migrations where source/target pairs don't overlap

## Example

Given a cluster with 3 shards (A, B, C) each with ~5461 slots, and 2 new empty shards (D, E):

```
Current: [5461, 5461, 5462, 0, 0]
Target:  [3277, 3277, 3277, 3277, 3276]
```

The tool generates batches like:

```
# === Batch 1 (run these 2 migrations concurrently) ===
kvctl migrate slot 3277-5460 --target 3 -n myns -c mycluster  # shard 0 -> 3
kvctl migrate slot 13829-16383 --target 4 -n myns -c mycluster  # shard 2 -> 4

# === Batch 2 ===
kvctl migrate slot 8554-10921 --target 3 -n myns -c mycluster  # shard 1 -> 3
...
```

Migrations in the same batch can run concurrently because no shard appears as both source and target within a batch.

## Input Format

The input is the JSON representation of a cluster from the kvrocks-controller API:

```json
{
  "name": "mycluster",
  "version": 1,
  "shards": [
    {
      "nodes": [...],
      "slot_ranges": ["0-5460"]
    },
    {
      "nodes": [...],
      "slot_ranges": ["5461-10921"]
    }
  ]
}
```
