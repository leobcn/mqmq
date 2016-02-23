package mqmq

import (
	"container/list"
)

type queue interface {
	enqueue() chan<- []byte
	requeue() chan<- []byte
	dequeue() <-chan []byte
	len() int
	stop()
}

type memoryQueue struct {
	chEnqueue chan []byte
	chRequeue chan []byte
	chDequeue chan []byte
	chLen     chan chan int
	chStop    chan struct{}
	data      *list.List
}

func newMemoryQueue() *memoryQueue {
	q := &memoryQueue{
		chEnqueue: make(chan []byte),
		chRequeue: make(chan []byte),
		chDequeue: make(chan []byte),
		chLen:     make(chan chan int),
		chStop:    make(chan struct{}),
		data:      list.New(),
	}
	go q.run()
	return q
}

func (q *memoryQueue) run() {
	for {
		if q.data.Len() == 0 {
			select {
			case v := <-q.chEnqueue:
				q.data.PushBack(v)
			case v := <-q.chRequeue:
				q.data.PushFront(v)
			case ch := <-q.chLen:
				ch <- 0
			case <-q.chStop:
				return
			}
		} else {
			select {
			case v := <-q.chEnqueue:
				q.data.PushBack(v)
			case v := <-q.chRequeue:
				q.data.PushFront(v)
			case q.chDequeue <- q.data.Front().Value.([]byte):
				q.data.Remove(q.data.Front())
			case ch := <-q.chLen:
				ch <- q.data.Len()
			case <-q.chStop:
				return
			}
		}
	}
}

func (q *memoryQueue) stop() {
	q.chStop <- struct{}{}
}

func (q *memoryQueue) len() int {
	ch := make(chan int)
	go func() { q.chLen <- ch }()
	return <-ch
}

func (q *memoryQueue) enqueue() chan<- []byte { return q.chEnqueue }
func (q *memoryQueue) requeue() chan<- []byte { return q.chRequeue }
func (q *memoryQueue) dequeue() <-chan []byte { return q.chDequeue }
