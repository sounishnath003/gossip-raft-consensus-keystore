package main

import (
	"context"
	"distirbuted-key-store/proto"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the first node.
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Put a key-value pair.
	_, err = c.Put(ctx, &proto.PutRequest{Key: "hello", Value: "world"})
	if err != nil {
		log.Fatalf("could not put: %v", err)
	}
	log.Printf("Put 'hello' -> 'world'")

	// Get the value for the key.
	r, err := c.Get(ctx, &proto.GetRequest{Key: "hello"})
	if err != nil {
		log.Fatalf("could not get: %v", err)
	}
	log.Printf("Get 'hello' -> '%s'", r.GetValue())
}
