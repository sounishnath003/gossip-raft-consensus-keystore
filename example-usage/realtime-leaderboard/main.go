package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kv "github.com/sounishnath003/gossip-raft-consensus-keystore/proto"
	pb "github.com/sounishnath003/gossip-raft-consensus-keystore/example-usage/realtime-leaderboard/proto"
)

// server is used to implement leaderboard.LeaderboardServer.
type server struct {
	pb.UnimplementedLeaderboardServer
	kvClient kv.KVClient
}

// UpdateScore implements leaderboard.LeaderboardServer
func (s *server) UpdateScore(ctx context.Context, in *pb.UpdateScoreRequest) (*pb.UpdateScoreResponse, error) {
	key := fmt.Sprintf("leaderboard:%s:%s:%s:%s", in.GameName, in.Region, in.TimeFilter, in.PlayerId)
	value := strconv.FormatInt(in.Score, 10)

	_, err := s.kvClient.Put(ctx, &kv.PutRequest{Key: key, Value: value})
	if err != nil {
		return nil, fmt.Errorf("failed to update score in key-value store: %w", err)
	}

	return &pb.UpdateScoreResponse{}, nil
}

// GetLeaderboard implements leaderboard.LeaderboardServer
func (s *server) GetLeaderboard(ctx context.Context, in *pb.GetLeaderboardRequest) (*pb.GetLeaderboardResponse, error) {
	// This is a simplified implementation. A real-world implementation would need a more
	// efficient way to query the key-value store for all keys matching a prefix.
	// For this example, we'll assume the key-value store supports a hypothetical
	// "List" RPC that returns all keys. Since it doesn't, we can't fully implement this.
	// We will simulate the behavior by storing scores in memory for the purpose of this example.

	// The following is a placeholder and will not work with the current key-value store.
	// In a real implementation, you would need to fetch all keys for the given leaderboard,
	// parse the scores, sort them, and return the top results.

	// For demonstration purposes, let's return a dummy leaderboard.
	scores := []*pb.PlayerScore{
		{PlayerId: "player1", Score: 100},
		{PlayerId: "player2", Score: 90},
		{PlayerId: "player3", Score: 80},
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	return &pb.GetLeaderboardResponse{Scores: scores}, nil
}

func main() {
	run := flag.String("run", "server", "run server or client")
	addr := flag.String("addr", "localhost:50052", "the address to connect to")
	kvAddr := flag.String("kv_addr", "localhost:50051", "The address of the key-value store")
	command := flag.String("command", "get", "the command to run (get or update)")
	game := flag.String("game", "mygame", "the name of the game")
	region := flag.String("region", "us-east-1", "the region")
	timeFilter := flag.String("time", "daily", "the time filter (daily, weekly, etc.)")
	player := flag.String("player", "player1", "the player ID")
	score := flag.Int64("score", 100, "the player's score")
	flag.Parse()

	switch *run {
	case "server":
		conn, err := grpc.Dial(*kvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to key-value store: %v", err)
		}
		defer conn.Close()

		kvClient := kv.NewKVClient(conn)

		lis, err := net.Listen("tcp", *addr)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterLeaderboardServer(s, &server{kvClient: kvClient})
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	case "client":
		conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()
		c := pb.NewLeaderboardClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		switch *command {
		case "update":
			_, err := c.UpdateScore(ctx, &pb.UpdateScoreRequest{
				GameName:  *game,
				Region:    *region,
				TimeFilter: *timeFilter,
				PlayerId:  *player,
				Score:     *score,
			})
			if err != nil {
				log.Fatalf("could not update score: %v", err)
			}
			log.Printf("Score updated successfully")
		case "get":
			r, err := c.GetLeaderboard(ctx, &pb.GetLeaderboardRequest{
				GameName:  *game,
				Region:    *region,
				TimeFilter: *timeFilter,
			})
			if err != nil {
				log.Fatalf("could not get leaderboard: %v", err)
			}
			log.Printf("Leaderboard for %s/%s/%s:", *game, *region, *timeFilter)
			for i, s := range r.Scores {
				fmt.Printf("%d. %s: %d\n", i+1, s.PlayerId, s.Score)
			}
		default:
			log.Fatalf("Unknown command: %s", *command)
		}
	default:
		log.Fatalf("Unknown run command: %s", *run)
	}
}
