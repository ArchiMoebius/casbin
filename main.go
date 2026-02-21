package main

import (
	"context"
	"crypto/rsa"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"kvservice/auth"
	pb "kvservice/pkg/gen/v1/kv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// ----------------- Context key (unexported type avoids collisions) -----------------
type contextKey string

const userIDKey contextKey = "userID"

// ----------------- KV Server -----------------
type KVServer struct {
	pb.UnimplementedKVServiceServer
	store    map[string]string
	mu       sync.RWMutex
	watchers map[string][]chan *pb.WatchResponse
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

	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key missing")
	}

	val, ok := s.store[req.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %s not found", req.Key)
	}

	return &pb.GetResponse{Value: val}, nil
}

// ListKeys returns every key currently in the store, sorted lexicographically.
// It is used by the REPL client as a completion source — no pagination needed
// for typical key-value store sizes, but a prefix filter is provided so the
// client can narrow candidates as the user types.
func (s *KVServer) ListKeys(ctx context.Context, req *pb.ListKeysRequest) (*pb.ListKeysResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.store))
	for k := range s.store {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return &pb.ListKeysResponse{Keys: keys}, nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" || req.Value == "" {
		return nil, status.Error(codes.InvalidArgument, "key or value missing")
	}

	// Store write — scoped lock, released before notification.
	s.mu.Lock()
	s.store[req.Key] = req.Value
	s.mu.Unlock()

	// Notify watchers — separate lock, never held at the same time as mu.
	userID, _ := ctx.Value(userIDKey).(string)
	now := time.Now().Unix()
	s.wmu.Lock()
	if chans, ok := s.watchers[req.Key]; ok {
		for _, ch := range chans {
			select {
			case ch <- &pb.WatchResponse{
				Key:       req.Key,
				Value:     req.Value,
				UserId:    userID,
				Timestamp: now,
			}:
			default: // skip if channel is full
			}
		}
	}
	s.wmu.Unlock()

	return &pb.PutResponse{Success: true}, nil
}

// ----------------- Watch Streaming -----------------
func (s *KVServer) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key missing")
	}

	ch := make(chan *pb.WatchResponse, 10)

	s.wmu.Lock()
	s.watchers[req.Key] = append(s.watchers[req.Key], ch)
	s.wmu.Unlock()

	defer func() {
		s.wmu.Lock()
		chans := s.watchers[req.Key]
		for i, c := range chans {
			if c == ch {
				s.watchers[req.Key] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		s.wmu.Unlock()
	}()

	for {
		select {
		case update, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// ----------------- Interceptors -----------------

// authServerStream wraps grpc.ServerStream to inject a context with userID.
type authServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authServerStream) Context() context.Context { return s.ctx }

// ----------------- Auth: shared core -----------------

// authenticate is the single auth implementation. Both the unary and stream
// interceptors delegate here. It extracts the credential from the incoming
// metadata, validates via the AuthManager, authorises against the policy,
// and returns a new context with the userID injected.
//
// auth.ValidateUser returns plain errors; this is the only place that maps
// them to gRPC status codes, keeping the auth package transport-agnostic.
func authenticate(ctx context.Context, fullMethod string, am *auth.AuthManager, methodAction map[string]string) (context.Context, error) {
	action, ok := methodAction[fullMethod]
	if !ok {
		return ctx, status.Errorf(codes.PermissionDenied, "unknown RPC method %s", fullMethod)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "missing metadata")
	}

	var authHeader string
	if headers := md.Get("authorization"); len(headers) > 0 {
		authHeader = headers[0]
	}

	userID, err := am.ValidateUser(authHeader)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, err.Error())
	}

	allowed, err := am.Authorize(userID, "kv", action)
	if err != nil {
		return ctx, status.Error(codes.Internal, err.Error())
	}
	if !allowed {
		return ctx, status.Error(codes.PermissionDenied, "forbidden")
	}

	return context.WithValue(ctx, userIDKey, userID), nil
}

// ----------------- Auth: unary + stream wrappers -----------------

func unaryAuthInterceptor(am *auth.AuthManager, methodAction map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := authenticate(ctx, info.FullMethod, am, methodAction)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamAuthInterceptor(am *auth.AuthManager, methodAction map[string]string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context(), info.FullMethod, am, methodAction)
		if err != nil {
			return err
		}
		return handler(srv, &authServerStream{ServerStream: ss, ctx: ctx})
	}
}

// ----------------- Logging: shared core -----------------

// logRequest is the single logging implementation. The caller passes in the
// method name, the context, and a thunk that runs the rest of the chain.
// Unary and stream wrappers differ only in how they call the next handler;
// the timing and formatting is identical.
func logRequest(ctx context.Context, fullMethod string, fn func() error) error {
	start := time.Now()
	err := fn()

	statusStr := "OK"
	if err != nil {
		statusStr = "ERROR"
	}

	userID, _ := ctx.Value(userIDKey).(string)
	if userID != "" {
		log.Printf("[%s] %s - user=%s - %v", statusStr, fullMethod, userID, time.Since(start))
	} else {
		log.Printf("[%s] %s - %v", statusStr, fullMethod, time.Since(start))
	}
	return err
}

// ----------------- Logging: unary + stream wrappers -----------------

func unaryLoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var resp interface{}
	err := logRequest(ctx, info.FullMethod, func() error {
		var herr error
		resp, herr = handler(ctx, req)
		return herr
	})
	return resp, err
}

func streamLoggingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return logRequest(ss.Context(), info.FullMethod, func() error {
		return handler(srv, ss)
	})
}

// ----------------- Interceptor chaining -----------------

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

func chainStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		chain := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			next := chain
			interceptor := interceptors[i]
			chain = func(currentSrv interface{}, currentSS grpc.ServerStream) error {
				return interceptor(currentSrv, currentSS, info, next)
			}
		}
		return chain(srv, ss)
	}
}

// ----------------- MAIN -----------------
func main() {
	// Basic auth credentials
	users := map[string]string{
		"alice": "password123",
		"bob":   "password456",
	}

	// JWT public keys — optional. Load if the file exists, otherwise nil.
	var jwtKeys map[string]*rsa.PublicKey
	pubKeyPath := "./config/public_key.pem"
	if _, err := os.Stat(pubKeyPath); err == nil {
		pubKey := auth.MustLoadPublicKey(pubKeyPath)
		jwtKeys = map[string]*rsa.PublicKey{
			"default": pubKey, // kid used in token generation
		}
		log.Println("✅ JWT public key loaded")
	} else {
		log.Println("ℹ️  JWT auth disabled (no public key found)")
	}

	authManager, err := auth.NewAuthManager("./config/model.conf", "./config/policy.csv", users, jwtKeys)
	if err != nil {
		log.Fatalf("Failed to init AuthManager: %v", err)
	}

	// Pull method->action map from proto descriptors instead of a hard-coded map.
	methodAction, err := auth.BuildMethodActionMap()
	if err != nil {
		log.Fatalf("Failed to build method action map: %v", err)
	}
	log.Printf("📋 Registered method actions: %v", methodAction)

	kvServer := NewKVServer()

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(chainUnaryInterceptors(
			// unaryAuthInterceptor(authManager, methodAction),
			unaryLoggingInterceptor,
		)),
		grpc.StreamInterceptor(chainStreamInterceptors(
			// streamAuthInterceptor(authManager, methodAction),
			streamLoggingInterceptor,
		)),
	)

	pb.RegisterKVServiceServer(grpcServer, kvServer)

	// Register reflection service — enables clients like grpcurl and the Python
	// reflection client to discover services, methods, and message types at runtime.
	reflection.Register(grpcServer)

	// Hot reload Casbin policies on SIGHUP
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	go func() {
		for range sighup {
			log.Println("Received SIGHUP -> reloading policies")
			authManager.ReloadPolicies()
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
