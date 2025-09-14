package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sounishnath003/gossip-raft-consensus-keystore/example-usage/realtime-leaderboard/proto"
)

func main() {
	numPlayers := flag.Int("n", 4, "number of players to ingest")
	addr := flag.String("addr", "localhost:50052", "the address to connect to")
	game := flag.String("game", "mygame", "the name of the game")
	region := flag.String("region", "us-east-1", "the region")
	timeFilter := flag.String("time", "daily", "the time filter (daily, weekly, etc.)")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewLeaderboardClient(conn)

	for {
		for i := 0; i < *numPlayers; i++ {
			playerID := "player" + strconv.Itoa(i+1)
			score := rand.Int63n(1000)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_, err := c.UpdateScore(ctx, &pb.UpdateScoreRequest{
				GameName:   *game,
				Region:     *region,
				TimeFilter: *timeFilter,
				PlayerId:   playerID,
				Score:      score,
			})
			if err != nil {
				log.Printf("could not update score for player %s: %v", playerID, err)
			} else {
				log.Printf("Score updated successfully for player %s with score %d in game %s and region %s", playerID, score, *game, *region)
			}
		}
		time.Sleep(1 * time.Second)
	}
}
