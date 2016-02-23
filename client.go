package mqmq

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"
)

// ErrBadResponse means that the client received an unexpected response from the server.
var ErrBadResponse = errors.New("mqmq: bad server response")

// ErrTimeout means that the given queue timeout expired and no message is received.
var ErrTimeout = errors.New("mqmq: timeout expired")

// Client is the mqmq client struct.
type Client struct {
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// NewClient creates a new mqmq client.
func NewClient() *Client {
	return &Client{}
}

// Connect connects to the server using TCP address addr.
// If addr is blank, DefaultAddr is used.
func (c *Client) Connect(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if addr == "" {
		addr = DefaultAddr
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)

	return nil
}

// SetConnection provides the client with an established net connection.
func (c *Client) SetConnection(conn net.Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn == nil {
		return errors.New("mqmq: nil conn")
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)

	return nil
}

func (c *Client) send(f frame) error {
	err := writeFrame(c.writer, f, maxFrameLen)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *Client) recv() (frame, error) {
	return readFrame(c.reader, maxFrameLen)
}

func (c *Client) cmd(request frame) (frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, errors.New("mqmq: client is not connected")
	}

	err := c.send(request)
	if err != nil {
		return nil, err
	}

	response, err := c.recv()
	if err != nil {
		return nil, err
	}

	return response, nil
}

// Put appends the message to the end of the given queue.
func (c *Client) Put(queue string, message []byte) error {
	if len(queue) > MaxQueueNameLen {
		return errors.New("mqmq: queue name length is larger than MaxQueueNameLen")
	}

	request := frame{bPut, []byte(queue), message}

	response, err := c.cmd(request)
	if err != nil {
		return err
	}

	if len(response) < 1 {
		return ErrBadResponse
	}
	if bytes.Equal(response[0], bError) {
		if len(response) < 2 {
			return ErrBadResponse
		}
		return errors.New("mqmq: server error response: " + string(response[1]))
	}
	if !bytes.Equal(response[0], bOK) {
		return ErrBadResponse
	}
	return nil
}

// Get receives the next message from the given queue.
// The timeout parameter specifies how much time to wait for the next message.
// The ErrTimeout error is returned if no new messages received from the queue
// for the given timeout. The maximum timeout value allowed is MaxGetTimeout.
func (c *Client) Get(queue string, timeout time.Duration) ([]byte, error) {
	if len(queue) > MaxQueueNameLen {
		return nil, errors.New("mqmq: queue name length is larger than MaxQueueNameLen")
	}

	if timeout < 0 {
		timeout = 0
	} else if timeout > MaxGetTimeout {
		return nil, errors.New("mqmq: timeout is larger than MaxGetTimeout")
	}
	timeoutStr := strconv.Itoa(int(timeout / time.Millisecond))

	request := frame{bGet, []byte(queue), []byte(timeoutStr)}

	response, err := c.cmd(request)
	if err != nil {
		return nil, err
	}

	if len(response) < 1 {
		return nil, ErrBadResponse
	}
	if bytes.Equal(response[0], bError) {
		if len(response) < 2 {
			return nil, ErrBadResponse
		}
		return nil, errors.New("mqmq: server error response: " + string(response[1]))
	}
	if bytes.Equal(response[0], bTimeout) {
		return nil, ErrTimeout
	}
	if !bytes.Equal(response[0], bOK) || len(response) < 2 {
		return nil, ErrBadResponse
	}
	return response[1], nil
}

// Info requests the server information.
func (c *Client) Info() (*ServerInfo, error) {
	request := frame{bInfo}

	response, err := c.cmd(request)
	if err != nil {
		return nil, err
	}

	if len(response) < 2 {
		return nil, ErrBadResponse
	}
	if bytes.Equal(response[0], bError) {
		return nil, errors.New("mqmq: server error response: " + string(response[1]))
	}
	if !bytes.Equal(response[0], bOK) {
		return nil, ErrBadResponse
	}

	info := &ServerInfo{}
	err = json.Unmarshal(response[1], info)
	if err != nil {
		return nil, ErrBadResponse
	}

	return info, nil
}

// Disconnect disconnects from the server.
func (c *Client) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	c.send(frame{bQuit})
	return c.conn.Close()
}
