# kvdog

An example distributed key-value store built with Go and gRPC.

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

Start a kvdog server with a configuration file:

```bash
./dist/kvserver -config hack/config.json
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
