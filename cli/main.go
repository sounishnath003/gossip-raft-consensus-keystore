// Package main implements a simple command-line client for the distributed key-value store.
package main

import (
	"context"
	"github.com/sounishnath003/gossip-raft-consensus-keystore/proto"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewKVClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Put a key-value pair into the store.
	_, err = c.Put(ctx, &proto.PutRequest{Key: "hello", Value: "world"})
	if err != nil {
		log.Fatalf("could not put: %v", err)
	}
	log.Printf("Put 'hello' -> 'world'")

	// Get the value for a key from the store.
	r, err := c.Get(ctx, &proto.GetRequest{Key: "hello"})
	if err != nil {
		log.Fatalf("could not get: %v", err)
	}
	log.Printf("Get 'hello' -> '%s'", r.GetValue())
}
