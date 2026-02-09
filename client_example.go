package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
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

	// Create a separate WaitGroup for the Watch goroutine
	var watchWg sync.WaitGroup
	watchWg.Add(1)

	// WATCH in a goroutine — streams updates for 5 seconds
	go func() {
		defer watchWg.Done()

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
				if err == io.EOF {
					log.Printf("[%s] Watch stream closed gracefully", username)
				} else if streamCtx.Err() == context.DeadlineExceeded {
					log.Printf("[%s] Watch timeout (5s) - expected behavior", username)
				} else {
					log.Printf("[%s] Watch error: %v", username, err)
				}
				return
			}
			log.Printf("[%s] WATCH update: key=%s value=%s user=%s ts=%d",
				username, update.Key, update.Value, update.UserId, update.Timestamp)
		}
	}()

	// Brief pause so the Watch stream is open before we Put
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

	// ✅ Wait for Watch to complete before returning
	watchWg.Wait()
}

// generateTraffic creates periodic updates to trigger Watch notifications
func generateTraffic(client pb.KVServiceClient, authHeader string, duration time.Duration) {
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", authHeader)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(duration)
	counter := 0

	log.Println("🔄 Starting traffic generator...")

	for {
		select {
		case <-ticker.C:
			counter++
			key := "foo"
			value := fmt.Sprintf("update-%d", counter)

			_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
			if err != nil {
				log.Printf("Traffic generator PUT failed: %v", err)
			} else {
				log.Printf("🔄 Traffic generator: PUT %s=%s", key, value)
			}

		case <-timeout:
			log.Println("🔄 Traffic generator stopped")
			return
		}
	}
}

// watchMultipleKeys demonstrates watching multiple different keys
func watchMultipleKeys(client pb.KVServiceClient, username, authHeader string, keys []string, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", authHeader)

	log.Printf("\n--- [%s] Watching multiple keys: %v ---", username, keys)

	var watchWg sync.WaitGroup

	// Start a watcher for each key
	for _, key := range keys {
		watchWg.Add(1)
		go func(k string) {
			defer watchWg.Done()

			streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			stream, err := client.Watch(streamCtx, &pb.WatchRequest{Key: k})
			if err != nil {
				log.Printf("[%s] WATCH failed for key %s: %v", username, k, err)
				return
			}

			for {
				update, err := stream.Recv()
				if err != nil {
					if streamCtx.Err() == context.DeadlineExceeded {
						log.Printf("[%s] Watch on %s timed out (expected)", username, k)
					} else {
						log.Printf("[%s] Watch on %s ended: %v", username, k, err)
					}
					return
				}
				log.Printf("[%s] WATCH[%s] update: value=%s user=%s ts=%d",
					username, update.Key, update.Value, update.UserId, update.Timestamp)
			}
		}(key)
	}

	// Give watchers time to start
	time.Sleep(100 * time.Millisecond)

	// Write to each key
	for i, key := range keys {
		_, err := client.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: fmt.Sprintf("value-%d", i),
		})
		if err != nil {
			log.Printf("[%s] PUT to %s failed: %v", username, key, err)
		} else {
			log.Printf("[%s] PUT %s=value-%d", username, key, i)
		}
		time.Sleep(200 * time.Millisecond)
	}

	watchWg.Wait()
}

func main() {
	conn, err := grpc.NewClient(
		"localhost:50052",
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
		{"foo", basicAuthHeader("foo", "wrongpass"), "Basic (invalid)"},
	}

	if jwtToken != "" {
		tests = append(tests, testCase{"charlie", bearerAuthHeader(jwtToken), "JWT"})
	}

	// Phase 1: Basic operations with single key watchers
	log.Println("\n========== PHASE 1: Basic Operations ==========")
	for _, tc := range tests {
		wg.Add(1)
		log.Printf("🔐 Starting %s auth test for %s", tc.authType, tc.username)
		go runClientOperations(client, tc.username, tc.authHeader, &wg)
	}

	wg.Wait()
	log.Println("\n✅ Phase 1 completed")

	// Small pause between phases
	time.Sleep(1 * time.Second)

	// Phase 2: Multiple key watchers with traffic generator
	log.Println("\n========== PHASE 2: Multiple Keys + Traffic Generator ==========")

	// Start traffic generator in background
	go generateTraffic(client, basicAuthHeader("alice", "password123"), 4*time.Second)

	// Wait a bit for traffic generator to start
	time.Sleep(200 * time.Millisecond)

	// Watch multiple keys
	wg.Add(1)
	go watchMultipleKeys(
		client,
		"bob",
		basicAuthHeader("bob", "password456"),
		[]string{"key1", "key2", "key3"},
		&wg,
	)

	wg.Wait()
	log.Println("\n✅ Phase 2 completed")

	// Phase 3: Rapid fire operations
	log.Println("\n========== PHASE 3: Rapid Fire Operations ==========")

	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"authorization",
		basicAuthHeader("alice", "password123"),
	)

	// Rapid PUT operations
	for i := 0; i < 10; i++ {
		_, err := client.Put(ctx, &pb.PutRequest{
			Key:   "rapid",
			Value: fmt.Sprintf("rapid-%d", i),
		})
		if err != nil {
			log.Printf("Rapid PUT %d failed: %v", i, err)
		} else {
			log.Printf("⚡ Rapid PUT %d: rapid=rapid-%d", i, i)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Rapid GET operations
	for i := 0; i < 5; i++ {
		resp, err := client.Get(ctx, &pb.GetRequest{Key: "rapid"})
		if err != nil {
			log.Printf("Rapid GET %d failed: %v", i, err)
		} else {
			log.Printf("⚡ Rapid GET %d: %s", i, resp.Value)
		}
		time.Sleep(30 * time.Millisecond)
	}

	log.Println("\n✅ Phase 3 completed")
	log.Println("\n🎉 All test phases completed - check proxy.log for detailed logs")
}
