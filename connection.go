package mqmq

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

// MaxGetTimeout is the maximum timeout value allowed for Get request.
const MaxGetTimeout = 1 * time.Hour

// MaxQueueNameLen is the maximum queue name length allowed.
const MaxQueueNameLen = 1024

const (
	maxGetTimeoutMsec = int(MaxGetTimeout / time.Millisecond)
	maxMsgLen         = 32 * 1024 * 1024
	maxFrameLen       = 4 + 3 + 4 + MaxQueueNameLen + 4 + maxMsgLen
)

var (
	bGet     = []byte("Get")
	bPut     = []byte("Put")
	bInfo    = []byte("Info")
	bQuit    = []byte("Quit")
	bOK      = []byte("OK")
	bError   = []byte("Error")
	bTimeout = []byte("Timeout")
)

type connection struct {
	server  *Server
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	stopped int32
	done    chan struct{}
}

func newConnection(server *Server, conn net.Conn) *connection {
	return &connection{
		server: server,
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		done:   make(chan struct{}),
	}
}

func (c *connection) run() {
	for c.running() {
		f, err := c.recv()
		if err != nil {
			if c.running() {
				c.server.logf("ERROR: failed to read frame (%s): %s", c.conn.RemoteAddr(), err)
				c.stop()
			}
			return
		}

		switch {
		case bytes.Equal(f[0], bGet):
			c.handleGet(f)
		case bytes.Equal(f[0], bPut):
			c.handlePut(f)
		case bytes.Equal(f[0], bInfo):
			c.handleInfo(f)
		case bytes.Equal(f[0], bQuit):
			c.handleQuit(f)
		default:
			c.handleUnknownCmd(f)
		}
	}
}

func (c *connection) running() bool {
	return atomic.LoadInt32(&c.stopped) == 0
}

func (c *connection) stop() {
	if !atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		return
	}
	close(c.done)
	c.conn.Close()
}

func (c *connection) send(f frame) error {
	err := writeFrame(c.writer, f, maxFrameLen)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *connection) recv() (frame, error) {
	return readFrame(c.reader, maxFrameLen)
}

func (c *connection) sendOrStop(f frame) {
	err := c.send(f)
	if err != nil && c.running() {
		c.server.logf("ERROR: failed to write frame (%s): %s", c.conn.RemoteAddr(), err)
		c.stop()
	}
}

// Request handler: Put <queue> <message>
func (c *connection) handlePut(f frame) {
	if len(f) < 3 {
		c.sendOrStop(frame{bError, []byte("REQUEST_BAD_PARAMS")})
		return
	}

	qname := string(f[1])
	if len(qname) > MaxQueueNameLen {
		c.sendOrStop(frame{bError, []byte("REQUEST_BAD_QUEUE_NAME")})
		return
	}

	message := f[2]

	q, err := c.server.getQueue(qname)
	if err != nil {
		return
	}

	select {
	case <-c.done:
		return
	case q.enqueue() <- message:
		c.sendOrStop(frame{bOK})
	}
}

// Request handler: Get <queue> <timeout>
func (c *connection) handleGet(f frame) {
	if len(f) < 2 {
		c.sendOrStop(frame{bError, []byte("REQUEST_BAD_PARAMS")})
		return
	}

	qname := string(f[1])
	if len(qname) > MaxQueueNameLen {
		c.sendOrStop(frame{bError, []byte("REQUEST_BAD_QUEUE_NAME")})
		return
	}

	timeoutMsec := 1
	if len(f) >= 3 {
		var err error
		timeoutMsec, err = strconv.Atoi(string(f[2]))
		if err != nil {
			c.sendOrStop(frame{bError, []byte("REQUEST_BAD_TIMEOUT")})
			return
		}
	}

	if timeoutMsec > maxGetTimeoutMsec {
		c.sendOrStop(frame{bError, []byte("REQUEST_BAD_TIMEOUT")})
		return
	}

	if timeoutMsec < 1 {
		timeoutMsec = 1
	}
	timeout := time.Duration(timeoutMsec) * time.Millisecond

	q, err := c.server.getQueue(qname)
	if err != nil {
		return
	}

	select {
	case <-c.done:
		return
	case message := <-q.dequeue():
		err = c.send(frame{bOK, []byte(message)})
		if err != nil {
			// Failed to send this message so lets put it back into the queue.
			select {
			case <-c.done:
				return
			case q.requeue() <- message:
			}
			if c.running() {
				c.server.logf("ERROR: failed to write frame (%s): %s", c.conn.RemoteAddr(), err)
				c.stop()
			}
		}
	case <-time.After(timeout):
		c.sendOrStop(frame{bTimeout})
	}
}

// Request handler: Info
func (c *connection) handleInfo(f frame) {
	info := c.server.Info()

	infoJSON, err := json.Marshal(info)
	if err != nil {
		c.server.logf("ERROR: failed to marshal json info (%s): %s", c.conn.RemoteAddr(), err)
		c.stop()
		return
	}

	c.sendOrStop(frame{bOK, infoJSON})
}

// Request handler: Quit
func (c *connection) handleQuit(f frame) {
	c.stop()
}

// Request handler: unknown command
func (c *connection) handleUnknownCmd(f frame) {
	c.send(frame{bError, []byte("REQUEST_UNKNOWN_COMMAND")})
	c.stop()
}
