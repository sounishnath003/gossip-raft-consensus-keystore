# Real-time Leaderboard

This project is an example of a real-time leaderboard service built on top of a distributed key-value store.

## Features

- **Update Score:** A client can update a player's score for a specific game, region, and time filter.
- **Get Leaderboard:** A client can retrieve the leaderboard for a specific game, region, and time filter. The leaderboard is sorted by score in descending order.
- **gRPC based:** The communication between the client and server is done using gRPC.
- **Distributed Key-Value Store:** The leaderboard data is stored in a distributed key-value store.

## How to run

### Prerequisites

- Go installed on your machine.
- A running instance of the distributed key-value store.

### Server

To start the leaderboard server, run the following command:

```bash
go run main.go --run server
```

By default, the server will listen on `localhost:50052` and connect to the key-value store at `localhost:50051`. You can change these addresses using the `--addr` and `--kv_addr` flags respectively.

### Client

The client can be used to update a player's score or get the leaderboard.

#### Update Score

To update a player's score, run the following command:

```bash
go run main.go --run client --command update --player <player_id> --score <score>
```

You can also specify the game, region, and time filter using the `--game`, `--region`, and `--time` flags.

#### Get Leaderboard

To get the leaderboard, run the following command:

```bash
go run main.go --run client --command get
```

You can also specify the game, region, and time filter for which you want the leaderboard using the `--game`, `--region`, and `--time` flags.
