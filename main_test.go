package main

import (
	"context"
	"testing"
	"time"
)

func TestClusterPing(t *testing.T) {
	config := Config{Nodes: []NodeId{1, 2}}
	ctx, cancel := context.WithCancel(context.Background())
	cluster := NewCluster(ctx, config)
	cluster.Run()
	cluster.Transport.Write(Message{
		Ping,
		Term{1, 0, time.Now()},
		SystemId,
		2,
		nil,
	})
	msg := <-cluster.Transport.Read(SystemId)
	if msg.Kind != Pong {
		t.Fatalf("expoected pong message but was %v", msg)
	}
	cancel()
}
