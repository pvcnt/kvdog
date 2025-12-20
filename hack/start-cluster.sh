#!/usr/bin/env bash

set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_ROOT/dist/kvserver"

# Array to store PIDs of background processes
PIDS=()

# Flag to prevent multiple cleanups
CLEANUP_DONE=0

# Cleanup function to kill all spawned processes
cleanup() {
    # Prevent multiple executions
    if [ $CLEANUP_DONE -eq 1 ]; then
        return
    fi
    CLEANUP_DONE=1

    echo ""
    echo "Shutting down cluster..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping process $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Wait a moment for graceful shutdown
    sleep 1

    # Force kill any remaining processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force stopping process $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    echo "Cluster stopped"
}

# Set up trap to catch signals and cleanup
trap cleanup SIGINT SIGTERM EXIT

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary not found at $BINARY"
    echo "Please build the server first: go build -o dist/kvserver ./cmd/kvserver"
    exit 1
fi

# Clean up old data
echo "Cleaning up old data..."
rm -rf /tmp/kvdog

# Start node 1 (bootstrap node)
echo "Starting node1 (bootstrap)..."
$BINARY -config "$SCRIPT_DIR/etc/node1.json" > /tmp/kvdog-node1.log 2>&1 &
PIDS+=($!)
echo "Node1 started (PID: ${PIDS[0]})"

# Wait a bit for bootstrap
sleep 2

# Start node 2
echo "Starting node2..."
$BINARY -config "$SCRIPT_DIR/etc/node2.json" > /tmp/kvdog-node2.log 2>&1 &
PIDS+=($!)
echo "Node2 started (PID: ${PIDS[1]})"

# Start node 3
echo "Starting node3..."
$BINARY -config "$SCRIPT_DIR/etc/node3.json" > /tmp/kvdog-node3.log 2>&1 &
PIDS+=($!)
echo "Node3 started (PID: ${PIDS[2]})"

echo ""
echo "==================================="
echo "3-node cluster started successfully"
echo "==================================="
echo ""
echo "Node PIDs: ${PIDS[*]}"
echo "Logs:"
echo "  - Node1: /tmp/kvdog-node1.log"
echo "  - Node2: /tmp/kvdog-node2.log"
echo "  - Node3: /tmp/kvdog-node3.log"
echo ""
echo "gRPC endpoints:"
echo "  - Node1: localhost:50051"
echo "  - Node2: localhost:50052"
echo "  - Node3: localhost:50053"
echo ""
echo "Press Ctrl+C to stop the cluster"
echo ""

# Wait for all background processes
# This will block until all processes exit or a signal is received
wait
