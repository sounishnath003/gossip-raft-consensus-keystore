package main

import (
	"context"
	"distirbuted-key-store/proto"
	"os"
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
	go startNode(50051, 12000, "/tmp/raft1", "node1", "", true)
	// Start the second node.
	go startNode(50052, 12001, "/tmp/raft2", "node2", "localhost:50051", false)

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
