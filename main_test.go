package main

import (
	"testing"
	"context"
	"time"
)

func Test_Main(t *testing.T) {
	config := Config{Nodes: []NodeId{1, 2}}
	ctx, cancel := context.WithCancel(context.Background())
	cluster := NewCluster(ctx, config)
	cluster.Run()
	cluster.Transport.Write(Message{
		Ping,
		Term{1, 0},
		1,
		2,
		nil,
	})

	time.Sleep(10 * time.Second)
	cancel()
}
