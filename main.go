package main

import (
	"context"
	"distirbuted-key-store/proto"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Store is a simple in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore creates a new Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Put sets a key-value pair in the store.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a value for a given key from the store.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

type server struct {
	proto.UnimplementedKVServer
	store    *Store
	ring     *Ring
	selfAddr string
	raft     *raft.Raft
}

func (s *server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if s.raft.State() != raft.Leader {
		var leader raft.ServerAddress
		for i := 0; i < 10; i++ { // Retry for 5 seconds
			leader = s.raft.Leader()
			if leader != "" {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if leader == "" {
			return nil, fmt.Errorf("leader not found")
		}

		leaderRaftAddr := string(leader)
		// HACK: This is a temporary solution for the test environment.
		parts := strings.Split(leaderRaftAddr, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid leader address format: %s", leaderRaftAddr)
		}
		raftPort, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid leader port: %s", parts[1])
		}
		grpcPort := raftPort - 12000 + 50051
		leaderGrpcAddr := fmt.Sprintf("localhost:%d", grpcPort)

		conn, err := grpc.Dial(leaderGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to leader: %w", err)
		}
		defer conn.Close()
		client := proto.NewKVClient(conn)
		return client.Put(ctx, req)
	}

	c := &command{
		Op:    "put",
		Key:   req.Key,
		Value: req.Value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, 500*time.Millisecond)
	if f.Error() != nil {
		return nil, f.Error()
	}

	return &proto.PutResponse{}, nil
}

func (s *server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	value, ok := s.store.Get(req.Key)
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	return &proto.GetResponse{Value: value}, nil
}

func (s *server) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	if s.raft.State() != raft.Leader {
		var leader raft.ServerAddress
		for i := 0; i < 10; i++ { // Retry for 5 seconds
			leader = s.raft.Leader()
			if leader != "" {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if leader == "" {
			return nil, fmt.Errorf("leader not found")
		}

		leaderRaftAddr := string(leader)
		// HACK: This is a temporary solution for the test environment.
		parts := strings.Split(leaderRaftAddr, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid leader address format: %s", leaderRaftAddr)
		}
		raftPort, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid leader port: %s", parts[1])
		}
		grpcPort := raftPort - 12000 + 50051
		leaderGrpcAddr := fmt.Sprintf("localhost:%d", grpcPort)

		conn, err := grpc.Dial(leaderGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to leader: %w", err)
		}
		defer conn.Close()
		client := proto.NewKVClient(conn)
		return client.Join(ctx, req)
	}

	f := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddress), 0, 0)
	if f.Error() != nil {
		return nil, f.Error()
	}

	// Add the new node to the ring.
	s.ring.AddNode(req.GrpcAddress)

	return &proto.JoinResponse{}, nil
}

func (s *server) Gossip(ctx context.Context, req *proto.GossipRequest) (*proto.GossipResponse, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	for key, value := range req.State {
		// A simple last-write-wins merge strategy.
		// In a real-world scenario, you might use vector clocks or other mechanisms
		// to resolve conflicts.
		s.store.data[key] = value
	}

	return &proto.GossipResponse{}, nil
}

func main() {
	port := flag.Int("port", 50051, "The server port")
	raftPort := flag.Int("raft_port", 12000, "The raft port")
	raftDir := flag.String("raft_dir", "/tmp/raft", "The raft data directory")
	nodeID := flag.String("node_id", "node1", "The node ID")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the cluster")
	joinAddr := flag.String("join_addr", "", "Address of a node to join")
	flag.Parse()

	startNode(*port, *raftPort, *raftDir, *nodeID, *joinAddr, *bootstrap)

	// Block forever to keep the main goroutine alive.
	select {}
}

func startNode(port, raftPort int, raftDir, nodeID, joinAddr string, bootstrap bool) (*grpc.Server, *raft.Raft) {
	addr := fmt.Sprintf("localhost:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Setup Raft.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	raftAddr := fmt.Sprintf("localhost:%d", raftPort)
	advertise, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		log.Fatalf("failed to resolve tcp addr: %v", err)
	}

	// Create the raft directory if it doesn't exist.
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		log.Fatalf("failed to create raft directory: %v", err)
	}

	// Setup Raft dependencies.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create bolt store: %v", err)
	}
	stableStore := logStore
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		log.Fatalf("failed to create snapshot store: %v", err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("failed to create tcp transport: %v", err)
	}

	store := NewStore()
	fsm := &fsm{store: store}
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("failed to create raft: %v", err)
	}

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	} else if joinAddr != "" {
		// Join an existing cluster.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			log.Fatalf("failed to connect to join address: %v", err)
		}
		defer conn.Close()
		client := proto.NewKVClient(conn)
		_, err = client.Join(context.Background(), &proto.JoinRequest{
			NodeId:      nodeID,
			RaftAddress: raftAddr,
			GrpcAddress: addr,
		})
		if err != nil {
			log.Fatalf("failed to join cluster: %v", err)
		}
	}

	s := grpc.NewServer()
	ring := NewRing(10)
	ring.AddNode(addr)

	server := &server{store: store, ring: ring, selfAddr: addr, raft: r}
	proto.RegisterKVServer(s, server)

	go server.startGossip()

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}()

	return s, r
}

func (s *server) startGossip() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.gossip()
	}
}

func (s *server) gossip() {
	// Select a random peer to gossip with.
	peer := s.ring.GetRandomNode(s.selfAddr)
	if peer == "" {
		return
	}

	// Connect to the peer.
	conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("failed to connect to peer %s: %v", peer, err)
		return
	}
	defer conn.Close()

	client := proto.NewKVClient(conn)

	s.store.mu.RLock()
	state := make(map[string]string)
	for k, v := range s.store.data {
		state[k] = v
	}
	s.store.mu.RUnlock()

	_, err = client.Gossip(context.Background(), &proto.GossipRequest{State: state})
	if err != nil {
		log.Printf("failed to gossip with peer %s: %v", peer, err)
	}
}
