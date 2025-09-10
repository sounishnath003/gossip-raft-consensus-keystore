package main

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Node represents a node in the cluster.
type Node struct {
	ID      string
	Address string
	State   map[string]string
	mu      sync.Mutex
}

// NewNode creates a new node.
func NewNode(id, address string) *Node {
	return &Node{
		ID:      id,
		Address: address,
		State:   make(map[string]string),
	}
}

// Gossip starts the gossip protocol for the node.
func (n *Node) Gossip(peers []*Node) {
	for {
		// Select a random peer to gossip with.
		peer := peers[rand.Intn(len(peers))]

		// Send the node's state to the peer.
		n.sendState(peer)

		// Sleep for a random amount of time.
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}
}

// sendState sends the node's state to a peer.
func (n *Node) sendState(peer *Node) {
	// Connect to the peer.
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	// Send the node's state to the peer.
	for key, value := range n.State {
		fmt.Fprintf(conn, "%s:%s\n", key, value)
	}
}

// handleConn handles an incoming connection.
func (n *Node) handleConn(conn net.Conn) {
	defer conn.Close()

	// Read the peer's state.
	for {
		var key, value string
		_, err := fmt.Fscanf(conn, "%s:%s\n", &key, &value)
		if err != nil {
			break
		}

		// Update the node's state.
		n.mu.Lock()
		n.State[key] = value
		n.mu.Unlock()
	}
}
