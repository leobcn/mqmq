MQMQ
====

[![GoDoc](https://godoc.org/github.com/disintegration/mqmq?status.svg)](https://godoc.org/github.com/disintegration/mqmq)

A very simple message queue client/server library and command-line tool written in Go.

Installation
------------

```
go get -u github.com/disintegration/mqmq
go get -u github.com/disintegration/mqmq/cmd/mqmq
```

The second command will install the mqmq command-line tool into the $GOBIN directory.

Command-line tool usage
-----------------------

Start the server using the default TCP address  (127.0.0.1:47774):
```
$ mqmq start
```

Print out the running server information:
```
$ mqmq info
Number of connections: 3
Number of queues: 2
Number of messages: 16
Queues:
        queue1: 10
        queue2: 6
```

The `mqmq` command also accepts the `-addr` flag:

```
$ mqmq start -addr 127.0.0.1:12345
```
```
$ mqmq info -addr 127.0.0.1:12345
```

To stop the server send the `SIGINT` or `SIGTERM` signal to the process.


Client examples
---------------

Example of a simple *producer* that sends 10 messages to the queue named "queue1":

```go
package main

import (
	"fmt"
	"log"

	"github.com/disintegration/mqmq"
)

func main() {
	c := mqmq.NewClient()
	err := c.Connect("")
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("message #%d", i)
		err := c.Put("queue1", []byte(msg))
		if err != nil {
			log.Fatalf("failed to put message: %s", err)
		}
		log.Printf("sent: %s", msg)
	}

	c.Disconnect()
}
```


Example of a simple *consumer* that receives 10 messages from the queue named "queue1":

```go
package main

import (
	"log"
	"time"

	"github.com/disintegration/mqmq"
)

func main() {
	c := mqmq.NewClient()
	err := c.Connect("")
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}

	for received := 0; received < 10; {
		// Wait at most 1 minute for the next message.
		msg, err := c.Get("queue1", 1*time.Minute)
		if err == mqmq.ErrTimeout {
			continue // No message so far. Keep on waiting.
		} else if err != nil {
			log.Fatalf("failed to get message: %s", err)
		}
		received++
		log.Printf("received: %s", string(msg))
	}

	c.Disconnect()
}
```

Protocol details
----------------

Client and server send `frames` to each other. Each frame is a sequence of (possibly binary) values.

```go 
type frame [][]byte
```

Frames are transfered using the following binary format:

1. Length of the frame, not including these 4 bytes (4 bytes - uint32, big-endian)
2. Length of the first value (4 bytes - uint32, big-endian)
3. The first value
4. Length of the second value (4 bytes - uint32, big-endian)
5. The second value
6. etc.

The first value of client frames is the command name, one of "Get", "Put", "Info" or "Quit".
The first value of server frames is the command result, one of "OK", "Error" or "Timeout" (for "Get" requests only).

#### Putting the message to a queue

```
client frame: Put, <queue name>, <message body>
server frame: OK
```

#### Getting the next message from a queue

```
client frame: Get, <queue name>, <timeout in milliseconds>
server frame: OK, <message body>
or
server frame: Timeout
```

#### Getting the server information

```
client frame: Info
server frame: OK, <server info>
```

The server info is a JSON-encoded structure containing some server metrics, e.g.: 

```
{"NumConnections": 1, "NumQueues": 1, "NumMessages": 10, "Queues": {"MyQueue": {"NumMessages": 10}}}
```

#### Disconnecting

```
client frame: Quit
<server closes connection>
```

#### Errors

Alternatively the server may respond with an error on any request.

```
server frame: Error, <error type>
```