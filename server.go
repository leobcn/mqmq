// Package mqmq implemets a simple message queue client/server.
package mqmq

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// DefaultAddr is the default TCP address for server listener.
const DefaultAddr = "127.0.0.1:47774"

// Server is a mqmq server struct.
type Server struct {
	logger      *log.Logger
	mu          sync.RWMutex
	state       ServerState
	listener    net.Listener
	queues      map[string]queue
	connections map[*connection]struct{}
}

// ServerState represents the current server state.
type ServerState int

// Server states.
const (
	ServerStateNew ServerState = iota
	ServerStateActive
	ServerStateStopped
)

var serverStateName = map[ServerState]string{
	ServerStateNew:     "new",
	ServerStateActive:  "active",
	ServerStateStopped: "stopped",
}

func (st ServerState) String() string {
	return serverStateName[st]
}

var errServerState = errors.New("mqmq: insufficient server state")

// NewServer creates a new mqmq server.
func NewServer() *Server {
	return &Server{}
}

// SetLogger sets the server logger.
func (s *Server) SetLogger(logger *log.Logger) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state != ServerStateNew {
		return errServerState
	}
	s.logger = logger
	return nil
}

// ListenAndServe listens on the TCP network address addr and handles client requests.
// If addr is blank, DefaultAddr is used.
func (s *Server) ListenAndServe(addr string) error {
	if addr == "" {
		addr = DefaultAddr
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logf("ERROR: net tcp listen (%s) failed: %s", addr, err)
		return err
	}

	return s.Serve(listener)
}

// Serve accepts incoming connections on the Listener l and handles client requests.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.state != ServerStateNew {
		s.mu.Unlock()
		return errServerState
	}
	s.state = ServerStateActive
	s.mu.Unlock()

	defer s.Stop()

	s.listener = l
	s.queues = make(map[string]queue)
	s.connections = make(map[*connection]struct{})

	s.logf("INFO: server started: %s", s.listener.Addr())

	for {
		conn, err := s.listener.Accept()
		s.mu.Lock()

		if s.state != ServerStateActive {
			s.mu.Unlock()
			return nil
		}

		if err != nil {
			s.mu.Unlock()
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				delay := 1 * time.Second
				s.logf("ERROR: listener accept temporary error: %s retrying in %v", err, delay)
				time.Sleep(delay)
				continue
			}
			s.logf("ERROR: listener accept error: %s", err)
			return err
		}

		c := newConnection(s, conn)
		s.connections[c] = struct{}{}
		s.mu.Unlock()

		go func() {
			c.run()
			s.mu.Lock()
			delete(s.connections, c)
			s.mu.Unlock()
		}()
	}
}

// Stop stops the server.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != ServerStateActive {
		return errServerState
	}
	s.state = ServerStateStopped

	if s.listener != nil {
		s.listener.Close()
	}

	if s.connections != nil {
		for c := range s.connections {
			delete(s.connections, c)
			c.stop()
		}
	}

	if s.queues != nil {
		for name, q := range s.queues {
			delete(s.queues, name)
			q.stop()
		}
	}

	s.logf("INFO: server stopped")
	return nil
}

// State returns the current server state.
func (s *Server) State() ServerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *Server) getQueue(name string) (q queue, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != ServerStateActive {
		return nil, errServerState
	}

	if q, ok := s.queues[name]; ok {
		return q, nil
	}

	q = newMemoryQueue()

	s.queues[name] = q
	return q, nil
}

func (s *Server) logf(format string, args ...interface{}) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// ServerInfo contains a server information.
type ServerInfo struct {
	NumConnections int
	NumQueues      int
	NumMessages    int
	Queues         map[string]ServerQueueInfo
}

// ServerQueueInfo contains a message queue information.
type ServerQueueInfo struct {
	NumMessages int
}

// Info returns the current server information.
func (s *Server) Info() ServerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := ServerInfo{
		NumConnections: len(s.connections),
		NumQueues:      len(s.queues),
		Queues:         make(map[string]ServerQueueInfo),
	}

	numMessages := 0
	for name, q := range s.queues {
		qlen := q.len()
		numMessages += qlen
		info.Queues[name] = ServerQueueInfo{NumMessages: qlen}
	}
	info.NumMessages = numMessages

	return info
}
