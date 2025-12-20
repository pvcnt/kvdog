# Hack Directory

This directory contains scripts and configuration files for development and testing.

## Scripts

### `start-cluster.sh`

Starts a 3-node kvdog cluster locally for development and testing.

**Usage:**
```bash
./hack/start-cluster.sh
```

**Features:**
- Automatically cleans up old data before starting
- Starts all three nodes in the background
- Displays node PIDs and log file locations
- Handles Ctrl+C gracefully by stopping all nodes
- Ensures all processes are terminated on exit

**Logs:**
- Node1: `/tmp/kvdog-node1.log`
- Node2: `/tmp/kvdog-node2.log`
- Node3: `/tmp/kvdog-node3.log`

**Monitoring:**
```bash
# Watch all logs at once
tail -f /tmp/kvdog-node*.log

# Watch individual node logs
tail -f /tmp/kvdog-node1.log
```

## Configuration Files

The `etc/` directory contains example configuration files for running kvdog nodes:

- **`node1.json`** - Bootstrap node configuration
  - Raft address: `127.0.0.1:7001`
  - gRPC address: `127.0.0.1:50051`
  - Data directory: `/tmp/kvdog/node1`
  - Bootstrap enabled (initializes the cluster)

- **`node2.json`** - Follower node configuration
  - Raft address: `127.0.0.1:7002`
  - gRPC address: `127.0.0.1:50052`
  - Data directory: `/tmp/kvdog/node2`

- **`node3.json`** - Follower node configuration
  - Raft address: `127.0.0.1:7003`
  - gRPC address: `127.0.0.1:50053`
  - Data directory: `/tmp/kvdog/node3`

## Testing with the Cluster

1. Start the cluster:
   ```bash
   ./hack/start-cluster.sh
   ```

2. In another terminal, interact with the cluster:
   ```bash
   # Connect to node 1 (leader)
   ./dist/kvctl -s localhost:50051 put foo bar

   # Connect to node 2 (follower)
   ./dist/kvctl -s localhost:50052 get foo

   # Connect to node 3 (follower)
   ./dist/kvctl -s localhost:50053 get foo
   ```

3. Watch the Raft logs to see consensus in action:
   ```bash
   tail -f /tmp/kvdog-node1.log
   ```

