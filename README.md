# kvdog

An example distributed key-value store built with Go, Raft and gRPC.

**This project is only for educational purposes, it should not be used in production!**

## Prerequisites

- Go 1.25.5 or later
- Protocol Buffers compiler (protoc) if regenerating proto files

## Building

Build both the server and client binaries:

```bash
make build
```

The binaries will be placed in the `dist/` directory, which is git-ignored.

You can also build them individually:

```bash
make server  # Build only the server
make client  # Build only the client
```

## Running the Server

### Single Node Mode

Start a single kvdog server with a configuration file:

```bash
./dist/kvserver -config hack/etc/node1.json
```

### Running a 3-Node Cluster

kvdog uses HashiCorp Raft for distributed consensus. To run a 3-node cluster locally:

#### Option 1: Using the Cluster Script (Recommended)

```bash
./hack/start-cluster.sh
```

You can then monitor the logs:
```bash
tail -f /tmp/kvdog-node1.log
tail -f /tmp/kvdog-node2.log
tail -f /tmp/kvdog-node3.log
   ```

#### Option 2: Manual Setup (Multiple Terminals)

Cleanup any previous data:
```bash
rm -rf /tmp/kvdog
```

Terminal 1 (Node 1 - Bootstrap node):
```bash
./dist/kvserver -config hack/etc/node1.json
```

Terminal 2 (Node 2):
```bash
./dist/kvserver -config hack/etc/node2.json
```

Terminal 3 (Node 3):
```bash
./dist/kvserver -config hack/etc/node3.json
```

## Using the Client

The `kvctl` CLI provides commands to interact with the kvdog server.

### Put a key-value pair

```bash
./dist/kvctl put mykey myvalue
```

### Get a value by key

```bash
./dist/kvctl get mykey
```

### Delete a key

```bash
./dist/kvctl delete mykey
```

### Connect to a different server

Use the `--server` (or `-s`) flag to specify a different server address:

```bash
./dist/kvctl -s localhost:50051 get mykey
```

## Development

### Run tests

```bash
make test
```

### Clean build artifacts

```bash
make clean
```

## License

See LICENSE file for details.
