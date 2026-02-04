package main

import (
	"context"
	"encoding/base64"
	"log"
	"sync"
	"time"

	"kvservice/auth"
	pb "kvservice/pkg/gen/v1/kv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ----------------- Helpers -----------------

func basicAuthHeader(username, password string) string {
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return "Basic " + token
}

func bearerAuthHeader(token string) string {
	return "Bearer " + token
}

func runClientOperations(client pb.KVServiceClient, username, authHeader string, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", authHeader)

	log.Printf("\n--- Running operations as %s ---", username)
	key := "foo"
	val := "bar"

	// WATCH in a goroutine — streams updates for 5 seconds
	go func() {
		streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		stream, err := client.Watch(streamCtx, &pb.WatchRequest{Key: key})
		if err != nil {
			log.Printf("[%s] WATCH failed to open: %v", username, err)
			return
		}

		for {
			update, err := stream.Recv()
			if err != nil {
				log.Printf("[%s] Watch ended: %v", username, err)
				return
			}
			log.Printf("[%s] WATCH update: key=%s value=%s user=%s ts=%d",
				username, update.Key, update.Value, update.UserId, update.Timestamp)
		}
	}()

	// Brief pause so the Watch stream is open before we Put.
	time.Sleep(100 * time.Millisecond)

	// PUT operation
	putResp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: val})
	if err != nil {
		log.Printf("[%s] PUT failed: %v", username, err)
	} else {
		log.Printf("[%s] PUT succeeded: %v", username, putResp.Success)
	}

	// GET operation
	getResp, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		log.Printf("[%s] GET failed: %v", username, err)
	} else {
		log.Printf("[%s] GET returned: %s", username, getResp.Value)
	}
}

func main() {
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	// Try to load private key for JWT generation (optional)
	var jwtToken string
	privateKeyPath := "./config/private_key.pem"
	if privateKey, err := auth.LoadPrivateKey(privateKeyPath); err == nil {
		// Generate a JWT for user "charlie" with email
		jwtToken, err = auth.GenerateJWT("charlie", "charlie@example.com", privateKey, "default")
		if err != nil {
			log.Fatalf("Failed to generate JWT: %v", err)
		}
		log.Println("✅ JWT token generated for charlie")
	} else {
		log.Println("ℹ️  JWT auth test skipped (no private key found)")
	}

	var wg sync.WaitGroup

	// Test cases: mix of Basic auth and JWT auth
	type testCase struct {
		username   string
		authHeader string
		authType   string
	}

	tests := []testCase{
		{"alice", basicAuthHeader("alice", "password123"), "Basic"},
		{"bob", basicAuthHeader("bob", "password456"), "Basic"},
		{"foo", basicAuthHeader("foo", "wrongpass"), "Basic (invalid)"}, // should fail
	}

	// Add JWT test case if we have a token
	if jwtToken != "" {
		tests = append(tests, testCase{"charlie", bearerAuthHeader(jwtToken), "JWT"})
	}

	for _, tc := range tests {
		wg.Add(1)
		log.Printf("🔐 Starting %s auth test for %s", tc.authType, tc.username)
		go runClientOperations(client, tc.username, tc.authHeader, &wg)
	}

	wg.Wait()
	log.Println("✅ All client operations completed")
}
