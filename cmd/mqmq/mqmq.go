package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/disintegration/mqmq"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	cmd := os.Args[1]

	flagset := &flag.FlagSet{Usage: printUsageAndExit}
	addr := flagset.String("addr", mqmq.DefaultAddr, "TCP address of the server")
	flagset.Parse(os.Args[2:])

	switch cmd {
	case "start":
		processStart(*addr)
	case "info":
		processInfo(*addr)
	default:
		printUsageAndExit()
	}
}

func processStart(addr string) {
	log.Printf("INFO: starting server: %s", addr)
	server := mqmq.NewServer()

	go func() {
		err := server.ListenAndServe(addr)
		if err != nil {
			log.Fatalf("FATAL: listen and serve failed: %s", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	s := <-c
	log.Printf("INFO: received signal: %v", s)

	server.Stop()
	log.Printf("INFO: server stopped: %s", addr)
}

func processInfo(addr string) {
	client := mqmq.NewClient()

	err := client.Connect(addr)
	if err != nil {
		fmt.Printf("Failed to connect to the server: %s\n", err)
		os.Exit(1)
	}

	info, err := client.Info()
	if err != nil {
		fmt.Printf("Failed get the server information: %s\n", err)
		os.Exit(1)
	}

	client.Disconnect()

	fmt.Printf("Number of connections: %d\n", info.NumConnections)
	fmt.Printf("Number of queues: %d\n", info.NumQueues)
	fmt.Printf("Number of messages: %d\n", info.NumMessages)

	if info.NumQueues > 0 {
		fmt.Println("Queues:")
		for qname, q := range info.Queues {
			fmt.Printf("        %s: %d\n", qname, q.NumMessages)
		}
	}
}

func printUsageAndExit() {
	usage := fmt.Sprintf(`mqmq is a tool to start the mqmq server or get the running server information.

usage:
     
     mqmq command [arguments]
     
commands:

    start       start the server
    info        get the server information
    
arguments:
    
    -addr       TCP address of the server (default is '%s')`, mqmq.DefaultAddr)

	fmt.Println(usage)
	os.Exit(1)
}
