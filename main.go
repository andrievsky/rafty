package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("Lets RAFT!")
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

	time.Sleep(20 * time.Second)
	cancel()
	time.Sleep(3 * time.Second)
}

type Cluster struct {
	Transport *Transport
	Nodes map[NodeId] *Node
} 

func NewCluster(ctx context.Context, config Config) *Cluster {
	transport := NewTransport(config)
	nodes := make(map[NodeId] *Node)
	for _, nodeId := range config.Nodes {
		nodes[nodeId] = NewNode(ctx, nodeId, transport)
	}
	return &Cluster{transport, nodes}
}

func (cluster *Cluster) Run() {
	for _, node := range cluster.Nodes {
		cluster.RunNode(node)
	}
}

func (cluster *Cluster) RunNode(node *Node) {
	go func() {
		err := node.Run()
		if err != nil {
			log(node, "terminated with error %v", err)
			return
		}
		log(node, "terminated")
	}()
}

type MessageKind int

const (
	VoteRequest MessageKind = iota
	VoteResponse
	NewTerm
	Ping
	Pong
)

type Term struct {
	Id     int
	Leader NodeId
}

type Message struct {
	Kind     MessageKind
	Term     Term
	Sender   NodeId
	Reciever NodeId
	Payload  any
}

type Config struct {
	Nodes []NodeId
}

type Transport struct {
	Config   Config
	Channels map[NodeId]chan Message
}

func NewTransport(config Config) *Transport {
	channels := make(map[NodeId]chan Message)
	for _, nodeId := range config.Nodes {
		channels[nodeId] = make(chan Message)
	}
	return &Transport{config, channels}
}

func (transport *Transport) Write(msg Message) {
	transport.Channels[msg.Reciever] <- msg
}

func (transport *Transport) Read(nodeId NodeId) <-chan Message {
	return transport.Channels[nodeId]
}

type NodeId int

const System = NodeId(0)

type Node struct {
	Ctx       context.Context
	Id        NodeId
	Transport *Transport
	Running   atomic.Bool
	Term      Term
	CancelTimeoutFn func()
}

func NewNode(ctx context.Context, id NodeId, transport *Transport) *Node {
	return &Node{Ctx: ctx, Id: id, Transport: transport}
}

func (node *Node) Run() error {
	log(node, "run")
	if node.Running.Swap(true) {
		return fmt.Errorf("node %v is already running", node.Id)
	}

	for {
		select {
		case <-node.Ctx.Done():
			{
				err := node.Shutdown()
				return err
			}
		case msg := <-node.Transport.Read(node.Id):
			{
				err := node.Process(msg)
				if err != nil {
					return err
				}
			}
		}
	}

}

func (node *Node) Process(msg Message) error {
	log(node, "process %v", msg)
	switch msg.Kind {
		case VoteRequest:
			log(node, "getting a vote request from %v", msg.Sender)
			break
		case Ping:
			log(node, "ping")
			node.SetTimeout(time.Second, func() {
				node.Send(Pong, msg.Sender, nil)
			})
			break
		case Pong:
			log(node, "pong")
			node.SetTimeout(time.Second, func() {
				node.Send(Ping, msg.Sender, nil)
			})
			break

	}
	return nil
}

func (node *Node) Send(kind MessageKind, reciver NodeId, payload any) {
	log(node, "send message of kind %v to %v", kind, reciver)
	node.Transport.Write(Message{
		kind, node.Term, node.Id, reciver, payload,
	})
}

func (node *Node) SetTimeout(duration time.Duration, callback func()) {
	node.CancelTimeout()
	node.CancelTimeoutFn = NewTimeout(node.Ctx, duration, callback)
}

func (node *Node) CancelTimeout() {
	if node.CancelTimeoutFn != nil {
		node.CancelTimeoutFn()
		node.CancelTimeoutFn = nil
	}
}

func (node *Node) Shutdown() error {
	log(node, "shutdown")
	return nil
}

func NewTimeout(ctx context.Context, duration time.Duration, callback func()) func() {
	internalCtx, cancel := context.WithTimeout(ctx, duration)
	go func() {
		<-internalCtx.Done()
		if internalCtx.Err() == context.DeadlineExceeded {
			callback()
		}
	}()
	return cancel
}

func log(node *Node, msg string, args ...any) {
	fmt.Printf("node %v: %s\n", node.Id, fmt.Sprintf(msg, args...))
}
