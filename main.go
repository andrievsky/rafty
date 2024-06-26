package main

import (
	"fmt"
	"context"
	"time"
	"sync/atomic"
)

func main() {
	fmt.Println("Lets RAFT!")
	
	ctx, cancel := context.WithCancel(context.Background())
	config := Config{}
	client := NewClient()

	node := NewNode(ctx, 1, client, config)

	go func() {
		err := node.Run()
		if err != nil {
			log(node, "terminated with error %w", err)
			return
		}
		log(node, "terminated")
		
	}()

	client.Write(Message("hello peer!"))

	time.Sleep(1 * time.Second)
	cancel()
	time.Sleep(3 * time.Second)
}

type Config struct {
	
}

type Client struct {
	Channel chan Message	
}

type Message string

func NewClient() *Client {
	return &Client{make(chan Message)}
}

func (client *Client) Read() <-chan Message {
	return client.Channel
}

func (client *Client) Write(msg Message) {
	client.Channel <- msg
}

type Node struct {
	Ctx context.Context
	Id int
	Client *Client
	Config Config
	Running atomic.Bool
}

func NewNode(ctx context.Context, id int, client *Client, config Config) *Node {
	return &Node{Ctx: ctx, Id: id, Client: client, Config: config}
}

func (node *Node) Run() error {
	log(node, "run")
	if node.Running.Swap(true) {
		return fmt.Errorf("node %s is already running", node.Id)
	}

	for {
		select {
			case <- node.Ctx.Done() : {
				err := node.Shutdown()
				return err		
			}
			case msg := <- node.Client.Read() : {
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
	return nil
}

func (node *Node) Shutdown() error {
	log(node, "shutdown")
	return nil
}

func log(node *Node, msg string, args ...any) {
	fmt.Printf("node %v: %s\n", node.Id, fmt.Sprintf(msg, args...))
}
