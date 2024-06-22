package main

import (
	"fmt"
	"context"
	"time"
)

func main() {
	fmt.Println("Lets RAFT!")
	network := Network{}
	ctx, cancel := context.WithCancel(context.Background())
	node := NewNode(ctx, 1, network)
	node.Init()
	node.Run()

	time.Sleep(time.Second)
	cancel()
}

type NodeID int

type Config struct {
	Nodes []int
}

type Network struct {
	Cnf Config
	//Bus MessageBus
}

type Message struct {
	Command Command
	Sender NodeID
	Reciever NodeID
}

const PromoteLeaderCommand = "PROMOTE LEADER"
const HeartBeatCommand = "HEART BEAT"
type Command struct {
	Type string
	Data any
}

func (t Network) Broadcast(msg Message) {
	
}

func (t Network) Pull() Message {
	time.Sleep(6 * time.Second)
	return (Message{Command{"test", "message"}, 0, 1})
}

type Term struct {
	Gen int
	Before int
	Leader NodeID
}

type Node struct {
	Ctx context.Context
	Term Term
	Network Network
	State *State
	Timeout Timeout
}

func NewNode(ctx context.Context, id NodeID, network Network) *Node {
	term := Term{}
	node := &Node{ctx, term, network, nil, Timeout{}}
	var leaderState *State
	var promoteState *State
	initState := NewState(id, "init", 
		func() {
			node.ChangeStateWithTimeout(promoteState, 3 * time.Second)
		},
		func(msg Message) {
			node.ChangeState(leaderState)
		},
		func() {
		},
	)
	promoteState = NewState(id, "promote", 
		func() {
		},
		func(msg Message) {
			node.ChangeState(leaderState)
		},
		func() {
		},
	)
	leaderState = NewState(id, "leader", 
		func() {
		},
		func(msg Message) {
		},
		func() {
		},
	)
	node.State = initState
	return node
}

func (node *Node) Init() {
	node.State.Enter()
}

func (node *Node) Run() {
	messageChan := NewMessageChan(node.Network)
	for {
		select {
			case  <- node.Ctx.Done() : node.Shutdown()
			case  <- node.Timeout.Ctx.Done() : {
				if node.Timeout.Ctx.Err() == context.DeadlineExceeded {
					node.ChangeState(node.Timeout.Next)
				}
			}
			case msg := <- messageChan : node.State.Process(msg)
		}
	}	
}

func (node *Node) ChangeState(next *State) {
	node.State.Exit()
	node.Timeout.Cancel()
	node.Timeout.Ctx = context.Background()
	node.Timeout.Cancel = func(){}
	node.Timeout.Next = nil
	node.State = next
	node.State.Enter()
}

func (node *Node) ChangeStateWithTimeout(next *State, timeout time.Duration) {
	if next == nil {
		panic("fallback state shouln not be empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	node.Timeout.Ctx = ctx
	node.Timeout.Cancel = cancel
	node.Timeout.Next = next
}

func (node Node) Shutdown() {
}

type EnterFn func()
type ProcessFn func(Message)
type ExitFn func()

func NewState(id NodeID, name string, enter EnterFn, process ProcessFn, exit ExitFn) *State {
	return &State{id, name, enter, process, exit}
}

type State struct {
	id NodeID
	name string
	enter EnterFn
	process ProcessFn
	exit ExitFn
}

func (t State) Enter() {
	log(t.id, t.name, "enter")
	t.enter()
}


func (t State) Process(msg Message) {
	log(t.id, t.name, fmt.Sprintf("got message %v", msg))
	t.process(msg)
}


func (t State) Exit() {
	log(t.id, t.name, "exit")
	t.exit()
}

func log(id NodeID, state string, msg string) {
	fmt.Printf("node %v (%s): %s\n", id, state, msg)
}

type Timeout struct {
	Ctx context.Context
	Cancel func()
	Next *State
}


func NewMessageChan(network Network) <- chan  Message {
	out := make(chan Message)
	go func() {
		for {
			msg := network.Pull()
			out <- msg
		}
	}()
	return out
}
