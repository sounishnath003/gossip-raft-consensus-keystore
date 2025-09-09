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
		leader := s.raft.Leader()
		// Forward the request to the leader.
		conn, err := grpc.NewClient(string(leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
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
		leader := s.raft.Leader()
		// Forward the request to the leader.
		conn, err := grpc.NewClient(string(leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		client := proto.NewKVClient(conn)
		return client.Join(ctx, req)
	}

	f := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddress), 0, 0)
	if f.Error() != nil {
		return nil, f.Error()
	}

	return &proto.JoinResponse{}, nil
}

func main() {
	port := flag.Int("port", 50051, "The server port")
	raftPort := flag.Int("raft_port", 12000, "The raft port")
	raftDir := flag.String("raft_dir", "/tmp/raft", "The raft data directory")
	nodeID := flag.String("node_id", "node1", "The node ID")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the cluster")
	joinAddr := flag.String("join_addr", "", "Address of a node to join")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Setup Raft.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)

	raftAddr := fmt.Sprintf("localhost:%d", *raftPort)
	advertise, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		log.Fatalf("failed to resolve tcp addr: %v", err)
	}

	// Create the raft directory if it doesn't exist.
	if err := os.MkdirAll(*raftDir, 0700); err != nil {
		log.Fatalf("failed to create raft directory: %v", err)
	}

	// Setup Raft dependencies.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(*raftDir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create bolt store: %v", err)
	}
	stableStore := logStore
	snapshotStore, err := raft.NewFileSnapshotStore(*raftDir, 2, os.Stderr)
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

	if *bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	} else if *joinAddr != "" {
		// Join an existing cluster.
		conn, err := grpc.NewClient(*joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to join address: %v", err)
		}
		defer conn.Close()
		client := proto.NewKVClient(conn)
		_, err = client.Join(context.Background(), &proto.JoinRequest{
			NodeId:      *nodeID,
			RaftAddress: raftAddr,
		})
		if err != nil {
			log.Fatalf("failed to join cluster: %v", err)
		}
	}

	s := grpc.NewServer()
	ring := NewRing(10)
	// In a real application, the node list would be managed dynamically.
	ring.AddNode("localhost:50051")
	ring.AddNode("localhost:50052")
	ring.AddNode("localhost:50053")

	proto.RegisterKVServer(s, &server{store: store, ring: ring, selfAddr: addr, raft: r})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
