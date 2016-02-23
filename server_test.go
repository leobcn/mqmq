package mqmq

import (
	"io/ioutil"
	"log"
	"net"
	"reflect"
	"testing"
	"time"
)

func startServer() (*Server, string) {
	s := NewServer()

	err := s.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	if err != nil {
		panic("Test server start failed: SetLogger: " + err.Error())
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("Test server start failed: net.Listen: " + err.Error())
	}
	addr := listener.Addr().String()

	go func() {
		err = s.Serve(listener)
		if err != nil {
			panic("Test server start failed: s.Serve" + err.Error())
		}
	}()

	return s, addr
}

func TestRequests(t *testing.T) {
	s, addr := startServer()
	defer s.Stop()

	qname := "test-queue"
	msg := "test-message"

	// Connect
	c := NewClient()
	err := c.Connect(addr)
	if err != nil {
		t.Fatalf("failed c.Connect: %s", err)
	}

	// Put message
	err = c.Put(qname, []byte(msg))
	if err != nil {
		t.Fatalf("failed c.Put: %s", err)
	}

	// Get Info
	info, err := c.Info()
	if err != nil {
		t.Fatalf("failed c.Info: %s", err)
	}
	expectInfo := &ServerInfo{
		NumConnections: 1,
		NumQueues:      1,
		NumMessages:    1,
		Queues: map[string]ServerQueueInfo{
			qname: {NumMessages: 1},
		},
	}
	if !reflect.DeepEqual(info, expectInfo) {
		t.Fatalf("failed c.Info: expected %#v, got %#v", expectInfo, info)
	}

	// Get message
	out, err := c.Get(qname, 1*time.Minute)
	if err != nil || string(out) != msg {
		t.Fatalf("failed c.Get: expected %#v, %#v, got %#v, %#v", msg, nil, string(out), err)
	}

	// Get - no more messages
	_, err = c.Get(qname, 10*time.Millisecond)
	if err != ErrTimeout {
		t.Fatalf("failed c.Get: expected error %#v, got %#v", ErrTimeout, err)
	}

	// Disconnect
	err = c.Disconnect()
	if err != nil {
		t.Fatalf("failed c.Disconnect: %s", err)
	}
}

func BenchmarkServerPutGet(b *testing.B) {
	s, addr := startServer()
	defer s.Stop()

	c := NewClient()
	err := c.Connect(addr)
	if err != nil {
		b.Fatalf("failed c.Connect: %s", err)
	}

	qname := "test-queue"
	msg := "test-message"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = c.Put(qname, []byte(msg))
		if err != nil {
			b.Fatalf("failed c.Put: %s", err)
		}

		out, err := c.Get(qname, 1*time.Minute)
		if err != nil || string(out) != msg {
			b.Fatalf("failed c.Get: expected %#v, %#v, got %#v, %#v", msg, nil, string(out), err)
		}
	}
}
