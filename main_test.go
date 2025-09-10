package main

import (
	"context"
	"distirbuted-key-store/proto"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGossip(t *testing.T) {
	// Clean up raft directories before and after the test.
	os.RemoveAll("/tmp/raft1")
	os.RemoveAll("/tmp/raft2")
	defer os.RemoveAll("/tmp/raft1")
	defer os.RemoveAll("/tmp/raft2")

	// Start the first node.
	s1, r1 := startNode(50051, 12000, "/tmp/raft1", "node1", "", true)
	defer s1.GracefulStop()
	defer r1.Shutdown()
	// Start the second node.
	s2, r2 := startNode(50052, 12001, "/tmp/raft2", "node2", "localhost:50051", false)
	defer s2.GracefulStop()
	defer r2.Shutdown()

	// Connect to the first node.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn1, err := grpc.DialContext(ctx, "localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	defer conn1.Close()
	client1 := proto.NewKVClient(conn1)

	// Connect to the second node.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	conn2, err := grpc.DialContext(ctx2, "localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	defer conn2.Close()
	client2 := proto.NewKVClient(conn2)

	// Put a key-value pair into the first node.
	_, err = client1.Put(context.Background(), &proto.PutRequest{Key: "foo", Value: "bar"})
	require.NoError(t, err)

	// Wait for the state to be gossiped.
	time.Sleep(2 * time.Second)

	// Get the key from the second node.
	resp, err := client2.Get(context.Background(), &proto.GetRequest{Key: "foo"})
	require.NoError(t, err)
	require.Equal(t, "bar", resp.Value)
}

var (
	benchClient proto.KVClient
	once        sync.Once
)

func benchmarkSetup(b *testing.B) {
	once.Do(func() {
		os.RemoveAll("/tmp/raft-bench")
		_, r := startNode(50061, 13000, "/tmp/raft-bench", "bench-node", "", true)

		// It's tricky to properly defer cleanup here, so we'll rely on the OS.
		// In a real-world scenario, you'd want a more robust cleanup mechanism.
		_ = r

		time.Sleep(2 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, "localhost:50061", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			b.Fatalf("failed to connect: %v", err)
		}
		benchClient = proto.NewKVClient(conn)

		// Pre-populate data for Get benchmarks.
		for i := 0; i < 1000; i++ {
			_, err := benchClient.Put(context.Background(), &proto.PutRequest{Key: fmt.Sprintf("key%d", i), Value: "value"})
			if err != nil {
				b.Fatalf("Put failed during pre-population: %v", err)
			}
		}
	})
}

func BenchmarkPut(b *testing.B) {
	benchmarkSetup(b)
	b.ResetTimer()
	// Start from 1000 to avoid overwriting pre-populated keys.
	for i := 1000; i < 1000+b.N; i++ {
		_, err := benchClient.Put(context.Background(), &proto.PutRequest{Key: fmt.Sprintf("key%d", i), Value: "value"})
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	benchmarkSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := benchClient.Get(context.Background(), &proto.GetRequest{Key: fmt.Sprintf("key%d", i%1000)})
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}
