package main

import (
	"context"
	"encoding/base64"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "kvservice/gen/go/proto/kv/v1"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ----------------- KV Server -----------------
type KVServer struct {
	pb.UnimplementedKVServiceServer
	store    map[string]string
	mu       sync.RWMutex
	watchers map[string][]chan *pb.WatchResponse // key -> list of watcher channels
	wmu      sync.Mutex
}

func NewKVServer() *KVServer {
	return &KVServer{
		store:    make(map[string]string),
		watchers: make(map[string][]chan *pb.WatchResponse),
	}
}

func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key missing")
	}
	val, ok := s.store[*req.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %s not found", *req.Key)
	}
	return &pb.GetResponse{Value: &val}, nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Key == nil || req.Value == nil {
		return nil, status.Error(codes.InvalidArgument, "key or value missing")
	}
	s.store[*req.Key] = *req.Value

	// Notify watchers
	s.wmu.Lock()
	userid := ctx.Value("userID").(string)
	now := time.Now().Unix()
	if chans, ok := s.watchers[*req.Key]; ok {
		for _, ch := range chans {
			select {
			case ch <- &pb.WatchResponse{
				Key:       req.Key,
				Value:     req.Value,
				UserId:    &userid,
				Timestamp: &now,
			}:
			default: // skip if channel is full
			}
		}
	}
	s.wmu.Unlock()
	status := true
	return &pb.PutResponse{Success: &status}, nil
}

// ----------------- Watch Streaming -----------------
func (s *KVServer) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	if req.Key == nil {
		return status.Error(codes.InvalidArgument, "key missing")
	}

	// Create watcher channel
	ch := make(chan *pb.WatchResponse, 10)

	s.wmu.Lock()
	s.watchers[*req.Key] = append(s.watchers[*req.Key], ch)
	s.wmu.Unlock()

	defer func() {
		// remove channel from watchers
		s.wmu.Lock()
		chans := s.watchers[*req.Key]
		for i, c := range chans {
			if c == ch {
				s.watchers[*req.Key] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		s.wmu.Unlock()
	}()

	// Stream updates
	for update := range ch {
		if err := stream.Send(update); err != nil {
			return err
		}
	}
	return nil
}

// ----------------- Auth Manager -----------------
type AuthManager struct {
	mu       sync.RWMutex
	enforcer *casbin.Enforcer
	users    map[string]string // username -> password
}

func NewAuthManager(modelPath, policyPath string, users map[string]string) (*AuthManager, error) {
	enforcer, err := casbin.NewEnforcer(modelPath, policyPath)
	if err != nil {
		return nil, err
	}
	return &AuthManager{enforcer: enforcer, users: users}, nil
}

func (am *AuthManager) ReloadPolicies(modelPath, policyPath string) {
	newEnforcer, err := casbin.NewEnforcer(modelPath, policyPath)
	if err != nil {
		log.Printf("Failed to reload policies: %v", err)
		return
	}
	if err := newEnforcer.LoadPolicy(); err != nil {
		log.Printf("Policy validation failed: %v", err)
		return
	}
	am.mu.Lock()
	am.enforcer = newEnforcer
	am.mu.Unlock()
	log.Println("✅ Casbin policies reloaded successfully")
}

func (am *AuthManager) ValidateUser(authHeader string) (string, error) {
	if authHeader == "" || !strings.HasPrefix(authHeader, "Basic ") {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	payload, err := base64.StdEncoding.DecodeString(authHeader[len("Basic "):])
	if err != nil {
		return "", status.Error(codes.Unauthenticated, "invalid base64")
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 {
		return "", status.Error(codes.Unauthenticated, "invalid basic auth format")
	}
	username, password := parts[0], parts[1]

	if stored, ok := am.users[username]; !ok || stored != password {
		return "", status.Error(codes.Unauthenticated, "invalid credentials")
	}

	return username, nil
}

func (am *AuthManager) Authorize(userID, obj, action string) (bool, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.enforcer.Enforce(userID, obj, action)
}

// ----------------- Interceptors -----------------
func authInterceptor(auth *AuthManager, methodAction map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		action, ok := methodAction[info.FullMethod]
		if !ok {
			return nil, status.Errorf(codes.PermissionDenied, "unknown RPC method %s", info.FullMethod)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		authHeaders := md.Get("authorization")
		var authHeader string
		if len(authHeaders) > 0 {
			authHeader = authHeaders[0]
		}

		userID, err := auth.ValidateUser(authHeader)
		if err != nil {
			return nil, err
		}

		allowed, err := auth.Authorize(userID, "kv", action)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !allowed {
			return nil, status.Error(codes.PermissionDenied, "forbidden")
		}

		ctx = context.WithValue(ctx, "userID", userID)
		return handler(ctx, req)
	}
}

func loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	statusStr := "OK"
	if err != nil {
		statusStr = "ERROR"
	}
	userID, _ := ctx.Value("userID").(string)
	if userID != "" {
		log.Printf("[%s] %s - user=%s - %v", statusStr, info.FullMethod, userID, time.Since(start))
	} else {
		log.Printf("[%s] %s - %v", statusStr, info.FullMethod, time.Since(start))
	}
	return resp, err
}

func chainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		chain := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			next := chain
			interceptor := interceptors[i]
			chain = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
				return interceptor(currentCtx, currentReq, info, next)
			}
		}
		return chain(ctx, req)
	}
}

// ----------------- MAIN -----------------
func main() {
	users := map[string]string{
		"alice": "password123",
		"bob":   "password456",
	}

	authManager, err := NewAuthManager("./config/model.conf", "./config/policy.csv", users)
	if err != nil {
		log.Fatalf("Failed to init AuthManager: %v", err)
	}

	kvServer := NewKVServer()
	methodAction := map[string]string{
		"/kv.v1.KVService/Get":   "get",
		"/kv.v1.KVService/Put":   "put",
		"/kv.v1.KVService/Watch": "watch",
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(chainUnaryInterceptors(
			authInterceptor(authManager, methodAction),
			loggingInterceptor,
		)),
	)

	pb.RegisterKVServiceServer(grpcServer, kvServer)

	// Hot reload Casbin policies
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	go func() {
		for range sighup {
			log.Println("Received SIGHUP -> reloading policies")
			authManager.ReloadPolicies("./config/model.conf", "./config/policy.csv")
		}
	}()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Println("🚀 gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
