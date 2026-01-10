package server

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

	pb "github.com/pvcnt/kvdog/internal/proto"
	"github.com/pvcnt/kvdog/internal/store"
)

const defaultTimeout = 5 * time.Second

// Node represents a Raft node
type Node struct {
	store  store.Store
	cfg    Config
	raft   *raft.Raft
	logger *log.Logger
}

// NewNode creates a new Raft node
func NewNode(cfg Config, store store.Store, transport raft.Transport, logger *log.Logger) (*Node, error) {
	// Set up metrics
	inm := metrics.NewInmemSink(10*time.Second, time.Minute)
	metricsConf := metrics.DefaultConfig("kvdog")
	metricsConf.EnableHostname = false
	_, err := metrics.NewGlobal(metricsConf, inm)
	if err != nil {
		return nil, fmt.Errorf("failed to set up metrics: %w", err)
	}

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   cfg.NodeID,
		Level:  hclog.Info,
		Output: os.Stderr,
	})

	// Create the log store and stable store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	// Create the snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create a new FSM (Finite State Machine).
	fsm := &kvFSM{store}

	// Create the Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	node := &Node{
		store:  store,
		cfg:    cfg,
		raft:   r,
		logger: logger,
	}

	// Bootstrap the cluster if this is the bootstrap node
	if cfg.Bootstrap {
		logger.Println("Bootstrapping cluster")
		servers := make([]raft.Server, len(cfg.Peers))
		for i, peer := range cfg.Peers {
			servers[i] = raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.RaftAddr),
			}
		}
		configuration := raft.Configuration{Servers: servers}
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			logger.Printf("Failed to bootstrap cluster: %v", err)
		} else {
			logger.Println("Cluster bootstrapped successfully")
		}
	}

	// Start a goroutine to monitor leadership changes
	go node.monitorLeadership()

	return node, nil
}

func (n *Node) Get(key string) ([]byte, bool) {
	return n.store.Get(key)
}

func (n *Node) Put(key string, value []byte) error {
	e := &pb.LogEntry{
		Cmd: &pb.LogEntry_SetCmd{
			SetCmd: &pb.LogEntry_Set{Key: key, Value: value},
		},
	}
	b, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}
	f := n.raft.Apply(b, defaultTimeout)
	return f.Error()
}

func (n *Node) Delete(key string) error {
	e := &pb.LogEntry{
		Cmd: &pb.LogEntry_DeleteCmd{
			DeleteCmd: &pb.LogEntry_Delete{Key: key},
		},
	}
	b, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}
	f := n.raft.Apply(b, defaultTimeout)
	return f.Error()
}

// monitorLeadership monitors and logs leadership changes
func (n *Node) monitorLeadership() {
	for {
		select {
		case isLeader := <-n.raft.LeaderCh():
			if isLeader {
				n.logger.Println("This node is now the leader")
			} else {
				addr, _ := n.raft.LeaderWithID()
				if addr == "" {
					n.logger.Println("No leader elected yet")
				} else {
					n.logger.Printf("Leader changed to %s", addr)
				}
			}
		}
	}
}

// IsLeader returns whether this node is the current leader
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// Leader returns the peer corresponding to the current leader
func (n *Node) Leader() (Peer, bool) {
	_, id := n.raft.LeaderWithID()
	for _, peer := range n.cfg.Peers {
		if peer.ID == string(id) {
			return peer, true
		}
	}
	return Peer{}, false
}

// Shutdown gracefully shuts down the Raft node
func (n *Node) Shutdown() error {
	n.logger.Println("Shutting down Raft node...")
	return n.raft.Shutdown().Error()
}

type kvFSM struct {
	s store.Store
}

func (f *kvFSM) Apply(log *raft.Log) interface{} {
	// Unmarshal the log entry
	var entry pb.LogEntry
	if err := proto.Unmarshal(log.Data, &entry); err != nil {
		panic(fmt.Sprintf("failed to unmarshal log entry: %v", err))
	}

	// Process the command
	switch {
	case entry.GetSetCmd() != nil:
		cmd := entry.GetSetCmd()
		if err := f.s.Set(cmd.GetKey(), cmd.GetValue()); err != nil {
			panic(fmt.Sprintf("failed to write key %q: %v", cmd.GetKey(), err))
		}
	case entry.GetDeleteCmd() != nil:
		cmd := entry.GetDeleteCmd()
		if err := f.s.Delete(cmd.GetKey()); err != nil {
			panic(fmt.Sprintf("failed to delete key %q: %v", cmd.GetKey(), err))
		}
	default:
		panic("unknown command in log entry")
	}

	return nil
}

func (f *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &kvSnapshot{f.s.Snapshot()}, nil
}

func (f *kvFSM) Restore(snapshot io.ReadCloser) error {
	if err := f.s.Restore(snapshot); err != nil {
		_ = snapshot.Close()
		return fmt.Errorf("failed to load snapshot: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %v", err)
	}
	return nil
}

type kvSnapshot struct {
	s store.Snapshot
}

func (s *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	if s.s == nil {
		return nil
	}
	if _, err := s.s.Write(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %v", err)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %v", err)
	}
	return nil
}

func (s *kvSnapshot) Release() {
	s.s = nil
}
