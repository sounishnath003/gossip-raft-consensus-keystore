package main

import (
	"hash/crc32"
	"math/rand"
	"sort"
	"strconv"
)

// Ring represents the consistent hashing ring.
type Ring struct {
	nodes     map[string]struct{}
	hashes    []uint32
	hashNode  map[uint32]string
	replicas  int
}

// NewRing creates a new consistent hashing ring.
func NewRing(replicas int) *Ring {
	return &Ring{
		nodes:    make(map[string]struct{}),
		hashes:   []uint32{},
		hashNode: make(map[uint32]string),
		replicas: replicas,
	}
}

// AddNode adds a node to the ring.
func (r *Ring) AddNode(node string) {
	r.nodes[node] = struct{}{}
	for i := 0; i < r.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(node + strconv.Itoa(i)))
		r.hashes = append(r.hashes, hash)
		r.hashNode[hash] = node
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// GetNode returns the node responsible for the given key.
func (r *Ring) GetNode(key string) string {
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
