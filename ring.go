// Package main implements a distributed key-value store.
// This file contains the implementation of a consistent hashing ring, which is
// used to distribute keys across the nodes in the cluster.
package main

import (
	"hash/crc32"
	"math/rand"
	"sort"
	"strconv"
	"sync"
)

// Ring represents the consistent hashing ring.
// It stores the nodes in the cluster and their corresponding hashes.
type Ring struct {
	nodes     map[string]struct{}
	hashes    []uint32
	hashNode  map[uint32]string
	replicas  int
	mu        sync.RWMutex
}

// NewRing creates a new consistent hashing ring with the given number of replicas.
func NewRing(replicas int) *Ring {
	return &Ring{
		nodes:    make(map[string]struct{}),
		hashes:   []uint32{},
		hashNode: make(map[uint32]string),
		replicas: replicas,
	}
}

// AddNode adds a node to the ring.
// It creates a number of virtual nodes (replicas) for the given node and
// adds them to the ring.
func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[node] = struct{}{}
	for i := 0; i < r.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(node + strconv.Itoa(i)))
		r.hashes = append(r.hashes, hash)
		r.hashNode[hash] = node
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// GetNode returns the node that is responsible for the given key.
// It uses consistent hashing to find the node that is closest to the key in the ring.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= hash })

	if idx == len(r.hashes) {
		idx = 0
	}

	return r.hashNode[r.hashes[idx]]
}

// GetRandomNode returns a random node from the ring, excluding the given node.
func (r *Ring) GetRandomNode(exclude string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodes))
	for node := range r.nodes {
		if node != exclude {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) == 0 {
		return ""
	}

	return nodes[rand.Intn(len(nodes))]
}
