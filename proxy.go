package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// ProxyServer is a transparent gRPC proxy that forwards requests to a backend
// while logging all traffic.
type ProxyServer struct {
	backendAddr string
	backendConn *grpc.ClientConn
	resolver    *ServiceResolver
	logFile     *os.File
}

// ServiceResolver uses gRPC reflection to discover backend service definitions.
type ServiceResolver struct {
	client   reflectionpb.ServerReflectionClient
	files    *protoregistry.Files
	messages *protoregistry.Types
}

// NewServiceResolver creates a resolver that queries the backend via reflection.
func NewServiceResolver(conn *grpc.ClientConn) (*ServiceResolver, error) {
	client := reflectionpb.NewServerReflectionClient(conn)
	files := &protoregistry.Files{}
	messages := &protoregistry.Types{}

	return &ServiceResolver{
		client:   client,
		files:    files,
		messages: messages,
	}, nil
}

// ResolveService loads the file descriptor for a service via reflection.
func (r *ServiceResolver) ResolveService(serviceName string) (protoreflect.ServiceDescriptor, error) {
	req := &reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: serviceName,
		},
	}

	stream, err := r.client.ServerReflectionInfo(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create reflection stream: %w", err)
	}
	defer stream.CloseSend()

	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send reflection request: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive reflection response: %w", err)
	}

	fdResp := resp.GetFileDescriptorResponse()
	if fdResp == nil {
		return nil, fmt.Errorf("no file descriptor in response")
	}

	// Load all file descriptors (including dependencies)
	for _, fdBytes := range fdResp.FileDescriptorProto {
		if err := r.loadFileDescriptor(fdBytes); err != nil {
			return nil, err
		}
	}

	// Find the service descriptor
	svcDesc, err := r.files.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, fmt.Errorf("service not found: %w", err)
	}

	serviceDesc, ok := svcDesc.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor is not a service")
	}

	return serviceDesc, nil
}

// loadFileDescriptor loads a single file descriptor and recursively loads its dependencies.
func (r *ServiceResolver) loadFileDescriptor(fdBytes []byte) error {
	// Unmarshal the file descriptor proto
	fdProto := &descriptorpb.FileDescriptorProto{}
	if err := proto.Unmarshal(fdBytes, fdProto); err != nil {
		return fmt.Errorf("failed to unmarshal file descriptor: %w", err)
	}

	// Check if already loaded
	if fdProto.Name != nil {
		if _, err := r.files.FindFileByPath(*fdProto.Name); err == nil {
			// Already loaded
			return nil
		}
	}

	// Load dependencies first
	for _, dep := range fdProto.Dependency {
		// Check if dependency is already loaded
		if _, err := r.files.FindFileByPath(dep); err == nil {
			continue
		}

		// Request the dependency
		depReq := &reflectionpb.ServerReflectionRequest{
			MessageRequest: &reflectionpb.ServerReflectionRequest_FileByFilename{
				FileByFilename: dep,
			},
		}

		stream, err := r.client.ServerReflectionInfo(context.Background())
		if err != nil {
			return fmt.Errorf("failed to create reflection stream for dependency %s: %w", dep, err)
		}

		if err := stream.Send(depReq); err != nil {
			stream.CloseSend()
			return fmt.Errorf("failed to send reflection request for dependency %s: %w", dep, err)
		}

		depResp, err := stream.Recv()
		stream.CloseSend()
		if err != nil {
			return fmt.Errorf("failed to receive reflection response for dependency %s: %w", dep, err)
		}

		depFdResp := depResp.GetFileDescriptorResponse()
		if depFdResp == nil {
			// Dependency might be a well-known type already in the registry
			continue
		}

		// Load all file descriptors in the dependency response
		for _, depFdBytes := range depFdResp.FileDescriptorProto {
			if err := r.loadFileDescriptor(depFdBytes); err != nil {
				return err
			}
		}
	}

	// Now build this file descriptor
	fd, err := protodesc.NewFile(fdProto, r.files)
	if err != nil {
		return fmt.Errorf("failed to create file descriptor for %s: %w", fdProto.GetName(), err)
	}

	if err := r.files.RegisterFile(fd); err != nil {
		// Already registered is OK
		return nil
	}

	// Register all message types
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		msg := msgs.Get(i)
		if err := r.messages.RegisterMessage(dynamicpb.NewMessageType(msg)); err != nil {
			// Already registered, ignore
			continue
		}
	}

	return nil
}

// GetMessageType returns a dynamic message type for the given message name.
func (r *ServiceResolver) GetMessageType(msgName string) (protoreflect.MessageType, error) {
	mt, err := r.messages.FindMessageByName(protoreflect.FullName(msgName))
	if err != nil {
		return nil, fmt.Errorf("message type not found: %w", err)
	}
	return mt, nil
}

// NewProxyServer creates a new proxy that forwards to the given backend address.
func NewProxyServer(backendAddr, logPath string) (*ProxyServer, error) {
	// Connect to backend
	conn, err := grpc.NewClient(
		backendAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to backend: %w", err)
	}

	// Create resolver
	resolver, err := NewServiceResolver(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create resolver: %w", err)
	}

	// Open log file
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return &ProxyServer{
		backendAddr: backendAddr,
		backendConn: conn,
		resolver:    resolver,
		logFile:     logFile,
	}, nil
}

// LogEntry represents a single RPC call logged to disk.
type LogEntry struct {
	Timestamp  string                 `json:"timestamp"`
	Service    string                 `json:"service"`
	Method     string                 `json:"method"`
	User       string                 `json:"user,omitempty"`
	Request    map[string]interface{} `json:"request,omitempty"`
	Response   map[string]interface{} `json:"response,omitempty"`
	Error      string                 `json:"error,omitempty"`
	DurationMs int64                  `json:"duration_ms"`
	Direction  string                 `json:"direction,omitempty"`  // "request" or "response"
	StreamMsg  bool                   `json:"stream_msg,omitempty"` // true if this is a streaming message
}

// logRPC writes an RPC call to the log file as JSON.
func (p *ProxyServer) logRPC(entry *LogEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("Failed to marshal log entry: %v", err)
		return
	}

	if _, err := p.logFile.Write(append(data, '\n')); err != nil {
		log.Printf("Failed to write log entry: %v", err)
	}
}

// extractUser gets the user from the Authorization header.
func extractUser(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "anonymous"
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "anonymous"
	}

	authHeader := authHeaders[0]

	if len(authHeader) > 6 && authHeader[:6] == "Basic " {
		payload, err := base64.StdEncoding.DecodeString(authHeader[6:])
		if err != nil {
			return "invalid-basic-auth"
		}

		parts := split(string(payload), ':')
		if len(parts) >= 1 {
			return parts[0]
		}
		return "malformed-basic-auth"
	}

	if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
		return "jwt-authenticated"
	}

	return "unknown-auth-type"
}

// messageToMap converts a proto message to a map for logging
func messageToMap(msg proto.Message) map[string]interface{} {
	var result map[string]interface{}
	if msg == nil {
		return result
	}
	reqJSON, err := protojson.Marshal(msg)
	if err != nil {
		return result
	}
	json.Unmarshal(reqJSON, &result)
	return result
}

// UnaryInterceptor intercepts unary RPCs for logging.
func (p *ProxyServer) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	user := extractUser(ctx)

	log.Printf("→ [%s] %s", user, info.FullMethod)

	// Log request
	var reqMap map[string]interface{}
	if msg, ok := req.(proto.Message); ok {
		reqMap = messageToMap(msg)
		// Log request body to file
		p.logRPC(&LogEntry{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Service:   info.FullMethod,
			Method:    info.FullMethod,
			User:      user,
			Request:   reqMap,
			Direction: "request",
		})
	}

	resp, err := handler(ctx, req)

	// Log response
	var respMap map[string]interface{}
	if msg, ok := resp.(proto.Message); ok {
		respMap = messageToMap(msg)
	}

	duration := time.Since(start)
	if err != nil {
		log.Printf("← [%s] %s FAILED: %v (%dms)", user, info.FullMethod, err, duration.Milliseconds())
	} else {
		log.Printf("← [%s] %s OK (%dms)", user, info.FullMethod, duration.Milliseconds())
	}

	// Log complete RPC to file
	entry := &LogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Service:    info.FullMethod,
		Method:     info.FullMethod,
		User:       user,
		Request:    reqMap,
		Response:   respMap,
		DurationMs: duration.Milliseconds(),
		Direction:  "response",
	}

	if err != nil {
		entry.Error = err.Error()
	}

	p.logRPC(entry)

	return resp, err
}

// loggingServerStream wraps grpc.ServerStream to log outgoing messages
type loggingServerStream struct {
	grpc.ServerStream
	proxy  *ProxyServer
	user   string
	method string
}

func (s *loggingServerStream) SendMsg(m interface{}) error {
	// Log the message being sent to client
	if msg, ok := m.(proto.Message); ok {
		msgMap := messageToMap(msg)
		s.proxy.logRPC(&LogEntry{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Service:   s.method,
			Method:    s.method,
			User:      s.user,
			Response:  msgMap,
			Direction: "response",
			StreamMsg: true,
		})
		log.Printf("  ← [%s] %s stream response: %v", s.user, s.method, msgMap)
	}
	return s.ServerStream.SendMsg(m)
}

func (s *loggingServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		// Log the message received from client
		if msg, ok := m.(proto.Message); ok {
			msgMap := messageToMap(msg)
			s.proxy.logRPC(&LogEntry{
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Service:   s.method,
				Method:    s.method,
				User:      s.user,
				Request:   msgMap,
				Direction: "request",
				StreamMsg: true,
			})
			log.Printf("  → [%s] %s stream request: %v", s.user, s.method, msgMap)
		}
	}
	return err
}

// StreamInterceptor intercepts streaming RPCs for logging.
func (p *ProxyServer) StreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	user := extractUser(ss.Context())

	log.Printf("→ [%s] %s (streaming)", user, info.FullMethod)

	// Wrap the stream to log messages
	wrappedStream := &loggingServerStream{
		ServerStream: ss,
		proxy:        p,
		user:         user,
		method:       info.FullMethod,
	}

	// Log stream start to file
	p.logRPC(&LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Service:   info.FullMethod,
		Method:    info.FullMethod,
		User:      user,
		Direction: "stream_start",
	})

	// Call handler with wrapped stream
	err := handler(srv, wrappedStream)

	duration := time.Since(start)
	if err != nil {
		log.Printf("← [%s] %s (streaming) FAILED: %v (%dms)", user, info.FullMethod, err, duration.Milliseconds())
	} else {
		log.Printf("← [%s] %s (streaming) ENDED (%dms)", user, info.FullMethod, duration.Milliseconds())
	}

	// Log stream end to file
	entry := &LogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Service:    info.FullMethod,
		Method:     info.FullMethod,
		User:       user,
		DurationMs: duration.Milliseconds(),
		Direction:  "stream_end",
	}

	if err != nil {
		entry.Error = err.Error()
	}

	p.logRPC(entry)
	return err
}

// UnknownServiceHandler creates a handler for any service.
func (p *ProxyServer) UnknownServiceHandler(
	srv interface{},
	stream grpc.ServerStream,
) error {
	fullMethod, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "failed to get method name")
	}

	// Check if this is a reflection request - forward it directly to backend
	if strings.HasPrefix(fullMethod, "/grpc.reflection.v1alpha.ServerReflection/") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection.v1.ServerReflection/") {
		return p.forwardReflectionRequest(stream, fullMethod)
	}

	// Parse service and method
	serviceName, methodName, err := parseFullMethod(fullMethod)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid method: %v", err)
	}

	// Resolve service descriptor
	serviceDesc, err := p.resolver.ResolveService(serviceName)
	if err != nil {
		return status.Errorf(codes.Unimplemented, "service not found: %v", err)
	}

	// Find method descriptor
	methodDesc := serviceDesc.Methods().ByName(protoreflect.Name(methodName))
	if methodDesc == nil {
		return status.Errorf(codes.Unimplemented, "method not found: %s", methodName)
	}

	// Get input/output message types
	inputType, err := p.resolver.GetMessageType(string(methodDesc.Input().FullName()))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get input type: %v", err)
	}

	outputType, err := p.resolver.GetMessageType(string(methodDesc.Output().FullName()))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get output type: %v", err)
	}

	// Forward metadata
	md, _ := metadata.FromIncomingContext(stream.Context())
	ctx := metadata.NewOutgoingContext(stream.Context(), md)

	// Handle based on streaming type
	if !methodDesc.IsStreamingClient() && !methodDesc.IsStreamingServer() {
		// Unary
		return p.forwardUnary(ctx, stream, fullMethod, inputType, outputType)
	} else if !methodDesc.IsStreamingClient() && methodDesc.IsStreamingServer() {
		// Server streaming
		return p.forwardServerStream(ctx, stream, fullMethod, inputType, outputType)
	}

	return status.Error(codes.Unimplemented, "client/bidirectional streaming not yet supported")
}

// forwardReflectionRequest forwards reflection requests directly to the backend
func (p *ProxyServer) forwardReflectionRequest(stream grpc.ServerStream, fullMethod string) error {
	// Forward metadata
	md, _ := metadata.FromIncomingContext(stream.Context())
	ctx := metadata.NewOutgoingContext(stream.Context(), md)

	// Create a bidirectional stream to the backend
	clientStream, err := p.backendConn.NewStream(
		ctx,
		&grpc.StreamDesc{
			StreamName:    "ServerReflectionInfo",
			ServerStreams: true,
			ClientStreams: true,
		},
		fullMethod,
	)
	if err != nil {
		return err
	}

	// Forward messages bidirectionally
	errChan := make(chan error, 2)

	// Forward client -> backend
	go func() {
		for {
			// Use a raw message type since we don't know the schema
			var msg interface{}
			if err := stream.RecvMsg(&msg); err != nil {
				if err == io.EOF {
					clientStream.CloseSend()
					errChan <- nil
					return
				}
				errChan <- err
				return
			}
			if err := clientStream.SendMsg(msg); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Forward backend -> client
	go func() {
		for {
			var msg interface{}
			if err := clientStream.RecvMsg(&msg); err != nil {
				if err == io.EOF {
					errChan <- nil
					return
				}
				errChan <- err
				return
			}
			if err := stream.SendMsg(msg); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Wait for either direction to error/complete
	return <-errChan
}

func (p *ProxyServer) forwardUnary(
	ctx context.Context,
	stream grpc.ServerStream,
	fullMethod string,
	inputType, outputType protoreflect.MessageType,
) error {
	req := inputType.New().Interface()
	if err := stream.RecvMsg(req); err != nil {
		return err
	}

	resp := outputType.New().Interface()
	err := p.backendConn.Invoke(ctx, fullMethod, req, resp)

	if err != nil {
		return err
	}

	return stream.SendMsg(resp)
}

func (p *ProxyServer) forwardServerStream(
	ctx context.Context,
	stream grpc.ServerStream,
	fullMethod string,
	inputType, outputType protoreflect.MessageType,
) error {
	req := inputType.New().Interface()
	if err := stream.RecvMsg(req); err != nil {
		return err
	}

	clientStream, err := p.backendConn.NewStream(
		ctx,
		&grpc.StreamDesc{ServerStreams: true},
		fullMethod,
	)
	if err != nil {
		return err
	}

	if err := clientStream.SendMsg(req); err != nil {
		return err
	}

	if err := clientStream.CloseSend(); err != nil {
		return err
	}

	// Forward all responses
	for {
		resp := outputType.New().Interface()
		err := clientStream.RecvMsg(resp)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := stream.SendMsg(resp); err != nil {
			return err
		}
	}
}

func parseFullMethod(fullMethod string) (service, method string, err error) {
	if len(fullMethod) == 0 || fullMethod[0] != '/' {
		return "", "", fmt.Errorf("invalid method format")
	}

	parts := split(fullMethod[1:], '/')
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid method format")
	}

	return parts[0], parts[1], nil
}

func split(s string, sep byte) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func main() {
	backendAddr := os.Getenv("BACKEND_ADDR")
	if backendAddr == "" {
		backendAddr = "localhost:50051"
	}

	proxyAddr := os.Getenv("PROXY_ADDR")
	if proxyAddr == "" {
		proxyAddr = ":50052"
	}

	logPath := os.Getenv("LOG_PATH")
	if logPath == "" {
		logPath = "./proxy.log"
	}

	log.Printf("🔄 Starting gRPC proxy")
	log.Printf("   Proxy address: %s", proxyAddr)
	log.Printf("   Backend address: %s", backendAddr)
	log.Printf("   Log file: %s", logPath)

	proxy, err := NewProxyServer(backendAddr, logPath)
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(proxy.UnaryInterceptor),
		grpc.StreamInterceptor(proxy.StreamInterceptor),
		grpc.UnknownServiceHandler(proxy.UnknownServiceHandler),
	)

	// Enable reflection on the proxy too
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", proxyAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("🚀 Proxy server ready")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
