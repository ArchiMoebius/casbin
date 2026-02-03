package main

import (
	"context"
	"encoding/base64"
	"log"
	"sync"
	"time"

	pb "kvservice/gen/go/proto/kv/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ----------------- Helpers -----------------
func basicAuthHeader(username, password string) string {
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return "Basic " + token
}

func runClientOperations(client pb.KVServiceClient, username, password string, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", basicAuthHeader(username, password))

	log.Printf("\n--- Running operations as %s ---", username)
	key := "foo"
	val := "bar"

	go func() {
		// WATCH operation (stream updates for 5 seconds)
		streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		stream, err := client.Watch(streamCtx, &pb.WatchRequest{Key: &key})
		if err != nil {
			log.Printf("[%s] WATCH failed: %v", username, err)
			return
		}

		for {
			update, err := stream.Recv()
			if err != nil {
				log.Printf("[%s] Watch ended: %v", username, err)
				break
			}
			log.Printf("[%s] WATCH update: key=%s value=%s user=%s ts=%d",
				username, *update.Key, *update.Value, *update.UserId, *update.Timestamp)
		}
	}()

	// PUT operation
	_, err := client.Put(ctx, &pb.PutRequest{Key: &key, Value: &val})
	if err != nil {
		log.Printf("[%s] PUT failed: %v", username, err)
	} else {
		log.Printf("[%s] PUT succeeded", username)
	}

	// GET operation
	getResp, err := client.Get(ctx, &pb.GetRequest{Key: &key})
	if err != nil {
		log.Printf("[%s] GET failed: %v", username, err)
	} else {
		log.Printf("[%s] GET returned: %s", username, *getResp.Value)
	}
}

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	var wg sync.WaitGroup
	users := []struct {
		username string
		password string
	}{
		{"alice", "password123"},
		{"bob", "password456"},
		{"foo", "wrongpass"}, // invalid user
	}

	for _, u := range users {
		wg.Add(1)
		go runClientOperations(client, u.username, u.password, &wg)
	}

	wg.Wait()
	log.Println("✅ All client operations completed")
}
