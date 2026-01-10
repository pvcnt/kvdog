package transport

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/yamux"
)

// MuxStreamLayer implements raft.StreamLayer using yamux for multiplexing
type MuxStreamLayer struct {
	bindAddr  string
	advertise net.Addr
	listener  net.Listener

	// Session management: peer address -> yamux session
	sessions     map[string]*yamux.Session
	sessionsLock sync.RWMutex

	// Stream routing: shard ID -> accept channel
	acceptCh     map[int]chan net.Conn
	acceptChLock sync.RWMutex

	// Lifecycle
	shutdown   bool
	shutdownCh chan struct{}
	shutdownWg sync.WaitGroup
	logger     hclog.Logger
}

// NewMuxStreamLayer creates a new multiplexing stream layer
func NewMuxStreamLayer(bindAddr string, advertise net.Addr, logger hclog.Logger) (*MuxStreamLayer, error) {
	// Bind TCP listener
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to %s: %w", bindAddr, err)
	}

	m := &MuxStreamLayer{
		bindAddr:   bindAddr,
		advertise:  advertise,
		listener:   listener,
		sessions:   make(map[string]*yamux.Session),
		acceptCh:   make(map[int]chan net.Conn),
		shutdownCh: make(chan struct{}),
		logger:     logger,
	}

	// Start accepting TCP connections
	m.shutdownWg.Add(1)
	go m.acceptLoop()

	m.logger.Info("mux stream layer started", "bind", bindAddr, "advertise", advertise)

	return m, nil
}

// Dial establishes a connection to the given address
// Address format: "host:port/shardID"
func (m *MuxStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	// Parse virtual address
	peerAddr, shardID, err := ParseVirtualAddress(string(address))
	if err != nil {
		return nil, err
	}

	// Get or create yamux session to peer
	session, err := m.getOrCreateSession(peerAddr, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to get session to %s: %w", peerAddr, err)
	}

	// Open stream on session
	stream, err := session.OpenStream()
	if err != nil {
		m.logger.Error("failed to open stream", "peer", peerAddr, "error", err)
		// Remove potentially stale session
		m.removeSession(peerAddr)
		return nil, fmt.Errorf("failed to open stream to %s: %w", peerAddr, err)
	}

	// Write protocol header with shard ID
	if err := WriteHeader(stream, shardID); err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	m.logger.Debug("dialed stream", "peer", peerAddr, "shard", shardID)

	return stream, nil
}

// Accept is not directly used - shards use their wrapper's Accept
func (m *MuxStreamLayer) Accept() (net.Conn, error) {
	return nil, fmt.Errorf("Accept should not be called on MuxStreamLayer directly")
}

// Close shuts down the stream layer
func (m *MuxStreamLayer) Close() error {
	m.sessionsLock.Lock()
	if m.shutdown {
		m.sessionsLock.Unlock()
		return nil
	}
	m.shutdown = true
	m.sessionsLock.Unlock()

	m.logger.Info("shutting down mux stream layer")

	// Close shutdown channel to signal goroutines
	close(m.shutdownCh)

	// Close listener to stop accepting new connections
	if err := m.listener.Close(); err != nil {
		m.logger.Error("error closing listener", "error", err)
	}

	// Close all yamux sessions
	m.sessionsLock.Lock()
	for peerAddr, session := range m.sessions {
		if err := session.Close(); err != nil {
			m.logger.Error("error closing session", "peer", peerAddr, "error", err)
		}
	}
	m.sessions = make(map[string]*yamux.Session)
	m.sessionsLock.Unlock()

	// Close all accept channels
	m.acceptChLock.Lock()
	for shardID, ch := range m.acceptCh {
		close(ch)
		m.logger.Debug("closed accept channel", "shard", shardID)
	}
	m.acceptCh = make(map[int]chan net.Conn)
	m.acceptChLock.Unlock()

	// Wait for goroutines to finish
	m.shutdownWg.Wait()

	m.logger.Info("mux stream layer shut down complete")

	return nil
}

// Addr returns the local address the layer is bound to
func (m *MuxStreamLayer) Addr() net.Addr {
	return m.advertise
}

// RegisterShard creates an accept channel for a specific shard
func (m *MuxStreamLayer) RegisterShard(shardID int) chan net.Conn {
	m.acceptChLock.Lock()
	defer m.acceptChLock.Unlock()

	ch := make(chan net.Conn, 16) // Buffered to avoid blocking
	m.acceptCh[shardID] = ch

	m.logger.Info("registered shard", "shard", shardID)

	return ch
}

// getOrCreateSession gets an existing session or creates a new one to the peer
func (m *MuxStreamLayer) getOrCreateSession(peerAddr string, timeout time.Duration) (*yamux.Session, error) {
	// Fast path: check if session exists with read lock
	m.sessionsLock.RLock()
	if m.shutdown {
		m.sessionsLock.RUnlock()
		return nil, fmt.Errorf("stream layer is shut down")
	}
	session, exists := m.sessions[peerAddr]
	m.sessionsLock.RUnlock()

	if exists && !session.IsClosed() {
		return session, nil
	}

	// Slow path: create new session with write lock
	m.sessionsLock.Lock()
	defer m.sessionsLock.Unlock()

	if m.shutdown {
		return nil, fmt.Errorf("stream layer is shut down")
	}

	// Double-check after acquiring write lock
	session, exists = m.sessions[peerAddr]
	if exists && !session.IsClosed() {
		return session, nil
	}

	// Create new TCP connection
	conn, err := net.DialTimeout("tcp", peerAddr, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", peerAddr, err)
	}

	// Create yamux client session
	config := yamuxConfig()
	session, err = yamux.Client(conn, config)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create yamux client: %w", err)
	}

	m.sessions[peerAddr] = session
	m.logger.Info("created yamux session", "peer", peerAddr)

	// Monitor session for closure
	m.shutdownWg.Add(1)
	go m.monitorSession(peerAddr, session)

	return session, nil
}

// removeSession removes a session from the map
func (m *MuxStreamLayer) removeSession(peerAddr string) {
	m.sessionsLock.Lock()
	defer m.sessionsLock.Unlock()

	if session, exists := m.sessions[peerAddr]; exists {
		session.Close()
		delete(m.sessions, peerAddr)
		m.logger.Info("removed session", "peer", peerAddr)
	}
}

// monitorSession monitors a session and removes it when it closes
func (m *MuxStreamLayer) monitorSession(peerAddr string, session *yamux.Session) {
	defer m.shutdownWg.Done()

	// Wait for session to close
	select {
	case <-session.CloseChan():
		m.logger.Info("session closed", "peer", peerAddr)
		m.sessionsLock.Lock()
		if m.sessions[peerAddr] == session {
			delete(m.sessions, peerAddr)
		}
		m.sessionsLock.Unlock()
	case <-m.shutdownCh:
		return
	}
}

// acceptLoop accepts incoming TCP connections and creates yamux server sessions
func (m *MuxStreamLayer) acceptLoop() {
	defer m.shutdownWg.Done()

	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.shutdownCh:
				return
			default:
				m.logger.Error("failed to accept connection", "error", err)
				continue
			}
		}

		peerAddr := conn.RemoteAddr().String()
		m.logger.Debug("accepted connection", "peer", peerAddr)

		// Create yamux server session
		m.shutdownWg.Add(1)
		go m.handleConnection(conn)
	}
}

// handleConnection handles an incoming TCP connection by creating a yamux session
// and accepting streams from it
func (m *MuxStreamLayer) handleConnection(conn net.Conn) {
	defer m.shutdownWg.Done()

	peerAddr := conn.RemoteAddr().String()

	// Create yamux server session
	config := yamuxConfig()
	session, err := yamux.Server(conn, config)
	if err != nil {
		m.logger.Error("failed to create yamux server", "peer", peerAddr, "error", err)
		conn.Close()
		return
	}

	m.logger.Info("created yamux server session", "peer", peerAddr)

	// Accept streams from this session
	m.shutdownWg.Add(1)
	go m.acceptStreams(peerAddr, session)
}

// acceptStreams accepts streams from a yamux session and routes them to shards
func (m *MuxStreamLayer) acceptStreams(peerAddr string, session *yamux.Session) {
	defer m.shutdownWg.Done()
	defer session.Close()

	for {
		stream, err := session.AcceptStream()
		if err != nil {
			select {
			case <-m.shutdownCh:
				return
			default:
				m.logger.Debug("session closed", "peer", peerAddr, "error", err)
				return
			}
		}

		// Read protocol header to determine shard
		shardID, err := ReadHeader(stream)
		if err != nil {
			m.logger.Error("invalid stream header", "peer", peerAddr, "error", err)
			stream.Close()
			continue
		}

		// Route stream to appropriate shard
		m.acceptChLock.RLock()
		ch, exists := m.acceptCh[shardID]
		m.acceptChLock.RUnlock()

		if !exists {
			m.logger.Warn("stream for unknown shard", "peer", peerAddr, "shard", shardID)
			stream.Close()
			continue
		}

		m.logger.Debug("routing stream", "peer", peerAddr, "shard", shardID)

		// Send stream to shard's accept channel
		select {
		case ch <- stream:
			// Stream successfully routed
		case <-m.shutdownCh:
			stream.Close()
			return
		default:
			// Channel full, close stream
			m.logger.Warn("accept channel full, dropping stream", "shard", shardID)
			stream.Close()
		}
	}
}

// yamuxConfig returns the yamux configuration with sensible defaults
func yamuxConfig() *yamux.Config {
	config := yamux.DefaultConfig()
	config.EnableKeepAlive = true
	config.KeepAliveInterval = 30 * time.Second
	config.ConnectionWriteTimeout = 10 * time.Second
	return config
}
