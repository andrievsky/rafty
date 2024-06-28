package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("Lets RAFT!")
	config := Config{
		TermDuration:         10 * time.Second,
		TermPromotionTimeout: 5 * time.Second,
		Nodes:                []NodeId{1, 2, 3},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cluster := NewCluster(ctx, config)
	cluster.Run()

	time.Sleep(60 * time.Second)
	cancel()
	time.Sleep(3 * time.Second)
}

type Cluster struct {
	Transport *Transport
	Nodes     map[NodeId]*Node
}

func NewCluster(ctx context.Context, config Config) *Cluster {
	transport := NewTransport(config)
	nodes := make(map[NodeId]*Node)
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
	NewLeader
	NextTerm
	Ping
	Pong
)

func prettyMessageKind(kind MessageKind) string {
	switch kind {
	case VoteRequest:
		return "VoteRequest"
	case VoteResponse:
		return "VoteResponse"
	case NewLeader:
		return "NewLeader"
	case NextTerm:
		return "NextTerm"
	case Ping:
		return "Ping"
	case Pong:
		return "Pong"
	}
	panic(fmt.Sprintf("unexpected message kind: %v", kind))
}

type Term struct {
	Id         int
	Leader     NodeId
	Expiration time.Time
}

func (term Term) Valid() bool {
	return term.Leader > 0 && time.Now().Before(term.Expiration)
}

type Message struct {
	Kind     MessageKind
	Term     Term
	Sender   NodeId
	Reciever NodeId
	Payload  any
}

type Config struct {
	TermDuration         time.Duration
	TermPromotionTimeout time.Duration
	Nodes                []NodeId
}

type Transport struct {
	Config   Config
	Channels map[NodeId]chan Message
}

func NewTransport(config Config) *Transport {
	channels := make(map[NodeId]chan Message)
	channels[SystemId] = make(chan Message)
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

const SystemId = NodeId(0)

type Node struct {
	Ctx             context.Context
	Id              NodeId
	Transport       *Transport
	Running         atomic.Bool // TODO Change to Status?
	Term            Term
	Next            Term
	CancelTimeoutFn func()
	VoteCounter     int
}

func NewNode(ctx context.Context, id NodeId, transport *Transport) *Node {
	return &Node{Ctx: ctx, Id: id, Transport: transport, Term: Term{0, 0, time.Now()}}
}

func (node *Node) Run() error {
	log(node, "run")
	if node.Running.Swap(true) {
		return fmt.Errorf("node %v is already running", node.Id)
	}
	node.StartNextTerm()

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
	log(node, "process %s: %v", prettyMessageKind(msg.Kind), msg)
	switch msg.Kind {
	case VoteRequest:
		if !node.Term.Valid() {
			node.Send(VoteResponse, msg.Sender, msg.Payload)
		}
		break
	case VoteResponse:
		if msg.Term.Id != node.Term.Id {
			break
		}
		node.VoteCounter++
		if node.VoteCounter > len(node.Transport.Config.Nodes)/2 {
			log(node, "consensus reached")
			node.VoteCounter = 0
			node.Term = msg.Payload.(Term)
			node.Broadcast(NewLeader, msg.Payload)
			node.NewLeader(msg.Payload.(Term))
		}
		break
	case NewLeader:
		node.NewLeader(msg.Payload.(Term))
		break
	case NextTerm:
		node.NextTerm(msg.Payload.(Term))
		break
	case Ping:
		node.SetTimeout(time.Second, func() {
			node.Send(Pong, msg.Sender, nil)
		})
		break

	}
	return nil
}

func (node *Node) NewLeader(term Term) {
	node.Next = term
	node.StartNextTerm()
}

func (node *Node) NextTerm(term Term) {
	node.Next = term
}

func (node *Node) StartNextTerm() {
	if node.Term.Leader == node.Id {
		log(node, "start next leader term")
		next := Term{node.Term.Id + 1, node.Id, node.Next.Expiration.Add(node.Transport.Config.TermDuration)}
		updateDuration := node.Next.Expiration.Add(-2 * time.Second).Sub(time.Now())
		node.Next = next
		node.SetTimeout(updateDuration, func() {
			node.Broadcast(NextTerm, node.Next)
			node.StartNextTerm()
		})
		return
	}
	if node.Next.Valid() {
		log(node, "start next term")
		node.Term = node.Next
		node.Next = Term{}
		node.SetTimeout(node.Term.Expiration.Sub(time.Now()), func() {
			node.StartNextTerm()
		})
		return
	}
	log(node, "waiting to be promoted")
	newTerm := Term{node.Term.Id + 1, node.Id, node.Term.Expiration.Add(node.Transport.Config.TermDuration)}
	promotionDelay := randomize(node.Transport.Config.TermPromotionTimeout)
	node.SetTimeout(promotionDelay, func() {
		node.Broadcast(VoteRequest, newTerm)
	})
}

func (node *Node) Send(kind MessageKind, reciver NodeId, payload any) {
	log(node, "send message %s to %v", prettyMessageKind(kind), reciver)
	node.Transport.Write(Message{
		kind, node.Term, node.Id, reciver, payload,
	})
}

func (node *Node) Broadcast(kind MessageKind, payload any) {
	log(node, "boadcast message %s", prettyMessageKind(kind))
	for _, nodeId := range node.Transport.Config.Nodes {
		if nodeId == node.Id {
			continue
		}
		node.Send(kind, nodeId, payload)
	}

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

func randomize(duration time.Duration) time.Duration {
	if duration <= 0 {
		return duration
	}
	return time.Duration(rand.Intn(int(duration)))
}
