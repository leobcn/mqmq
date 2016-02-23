package mqmq

import (
	"bytes"
	"testing"
)

func TestMemoryQueue(t *testing.T) {
	q := newMemoryQueue()
	testQueue(t, q)
}

func testQueue(t *testing.T, q queue) {
	n := q.len()
	if n != 0 {
		t.Errorf("failed test-init-len: expected 0, got %d", n)
	}

	messages := [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}

	for i, m := range messages {
		q.enqueue() <- m
		n = q.len()
		if n != i+1 {
			t.Errorf("failed test-put-len: expected %d, got %d", i+1, n)
		}
	}

	for i, m := range messages {
		v := <-q.dequeue()
		if bytes.Compare(v, m) != 0 {
			t.Errorf("failed test-get-value: expected %v, got %v", m, v)
		}
		n = q.len()
		if n != len(messages)-i-1 {
			t.Errorf("failed test-get-len: expected %d, got %d", len(messages)-i-1, n)
		}
	}

	for i, m := range messages {
		q.requeue() <- m
		n = q.len()
		if n != i+1 {
			t.Errorf("failed test-put-len: expected %d, got %d", i+1, n)
		}
	}

	for i := range messages {
		v := <-q.dequeue()
		if bytes.Compare(v, messages[len(messages)-i-1]) != 0 {
			t.Errorf("failed test-get-value: expected %v, got %v", messages[len(messages)-i-1], v)
		}
		n = q.len()
		if n != len(messages)-i-1 {
			t.Errorf("failed test-get-len: expected %d, got %d", len(messages)-i-1, n)
		}
	}

	q.stop()
}

func BenchmarkMemoryQueueEnqDeq(b *testing.B) {
	q := newMemoryQueue()
	benchQueueEnqDeq(b, q)
}

func BenchmarkMemoryQueueReqDeq(b *testing.B) {
	q := newMemoryQueue()
	benchQueueReqDeq(b, q)
}

func benchQueueEnqDeq(b *testing.B, q queue) {
	data := []byte{0x00}
	for i := 0; i < 1000; i++ {
		q.enqueue() <- data
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.enqueue() <- data
		<-q.dequeue()
	}
}

func benchQueueReqDeq(b *testing.B, q queue) {
	data := []byte{0x00}
	for i := 0; i < 1000; i++ {
		q.enqueue() <- data
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.requeue() <- data
		<-q.dequeue()
	}
}
