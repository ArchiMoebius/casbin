package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"sync"
	"syscall"
	"time"

	"kvservice/auth"
	pbAudit "kvservice/pkg/gen/v1/audit"
	pbConfig "kvservice/pkg/gen/v1/config"
	pbKV "kvservice/pkg/gen/v1/kv"
	pbUser "kvservice/pkg/gen/v1/user"

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
	pbKV.UnimplementedKVServiceServer
	store    map[string]string
	mu       sync.RWMutex
	watchers map[string][]chan *pbKV.WatchResponse
	wmu      sync.Mutex
}

func NewKVServer() *KVServer {
	return &KVServer{
		store:    make(map[string]string),
		watchers: make(map[string][]chan *pbKV.WatchResponse),
	}
}

func (s *KVServer) Get(ctx context.Context, req *pbKV.GetRequest) (*pbKV.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key missing")
	}

	val, ok := s.store[req.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %s not found", req.Key)
	}

	return &pbKV.GetResponse{Value: val}, nil
}

// ListKeys returns every key currently in the store, sorted lexicographically.
// It is used by the REPL client as a completion source — no pagination needed
// for typical key-value store sizes, but a prefix filter is provided so the
// client can narrow candidates as the user types.
func (s *KVServer) ListKeys(ctx context.Context, req *pbKV.ListKeysRequest) (*pbKV.ListKeysResponse, error) {
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

	return &pbKV.ListKeysResponse{Keys: keys}, nil
}

func (s *KVServer) Put(ctx context.Context, req *pbKV.PutRequest) (*pbKV.PutResponse, error) {
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
			case ch <- &pbKV.WatchResponse{
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

	return &pbKV.PutResponse{Success: true}, nil
}

// ----------------- Watch Streaming -----------------
func (s *KVServer) Watch(req *pbKV.WatchRequest, stream pbKV.KVService_WatchServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key missing")
	}

	ch := make(chan *pbKV.WatchResponse, 10)

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

const defaultSearchLimit = 1000

// SearchRegex returns every entry whose KEY matches the given RE2 pattern,
// along with its stored value.  Results are sorted by key.
//
// Use anchors for precision:
//   - "^foo"   keys starting with "foo"
//   - "foo$"   keys ending with "foo"
//   - "^foo$"  exact key match (prefer SearchExact for this)
//
// Example (REPL):
//
//	key.search.regex --pattern "^prod-" --limit 50
func (s *KVServer) SearchRegex(
	ctx context.Context, req *pbKV.SearchRegexRequest,
) (*pbKV.SearchRegexResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if req.Pattern == "" {
		return nil, status.Error(codes.InvalidArgument, "pattern must not be empty")
	}

	re, err := regexp.Compile(req.Pattern)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid RE2 pattern %q: %v", req.Pattern, err)
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = defaultSearchLimit
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect matching keys, sort for deterministic output, then truncate.
	var keys []string
	for k := range s.store {
		if re.MatchString(k) { // match against the KEY name
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	results := make([]*pbKV.SearchResult, 0, min(len(keys), limit))
	for _, k := range keys {
		if len(results) >= limit {
			break
		}
		results = append(results, &pbKV.SearchResult{Key: k, Value: s.store[k]})
	}

	return &pbKV.SearchRegexResponse{Results: results}, nil
}

// SearchExact returns every entry whose KEY exactly equals req.Key,
// along with its stored value.
//
// In a plain map store this will return at most one result, but the
// repeated response shape keeps it consistent with SearchRegex and
// leaves room for sharded / multi-store backends.
//
// Example (REPL):
//
//	key.search.exact --key foo
func (s *KVServer) SearchExact(
	ctx context.Context, req *pbKV.SearchExactRequest,
) (*pbKV.SearchExactResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = defaultSearchLimit
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all keys that exactly match, sort for consistency.
	var keys []string
	for k := range s.store {
		if k == req.Key { // match against the KEY name
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	results := make([]*pbKV.SearchResult, 0, min(len(keys), limit))
	for _, k := range keys {
		if len(results) >= limit {
			break
		}
		results = append(results, &pbKV.SearchResult{Key: k, Value: s.store[k]})
	}

	return &pbKV.SearchExactResponse{Results: results}, nil
}

// ----------------- User Services ----------------

// UserServer implements pb.UserServiceServer.
type UserServer struct {
	pbUser.UnimplementedUserServiceServer

	mu     sync.RWMutex
	users  map[int32]*pbUser.User
	nextID int32
}

// NewUserServer returns a UserServer seeded with demo data.
func NewUserServer() *UserServer {
	s := &UserServer{
		users:  make(map[int32]*pbUser.User),
		nextID: 4,
	}
	now := time.Now().Unix()
	s.users[1] = &pbUser.User{Id: 1, Username: "alice", Email: "alice@example.com", Role: "admin", Active: true, CreatedAt: now - 86400*30}
	s.users[2] = &pbUser.User{Id: 2, Username: "bob", Email: "bob@example.com", Role: "editor", Active: true, CreatedAt: now - 86400*14}
	s.users[3] = &pbUser.User{Id: 3, Username: "carol", Email: "carol@example.com", Role: "viewer", Active: false, CreatedAt: now - 86400*7}
	return s
}

// ── ListUsers ─────────────────────────────────────────────────────────────────
// Bootstrap RPC. Returns all users sorted by ID.
// The REPL projects this list into the multi-column completion dropdown:
//
//	id  │  username  │  role    │  ✓
//	────────────────────────────────
//	1   │  alice     │  admin   │  ✔
//	2   │  bob       │  editor  │  ✔
//	3   │  carol     │  viewer  │  ✘
//
// The "active" field renders as ✔/✘ via CELL_BOOL_ICON.
// Selecting a row inserts the numeric id (value_field: "id").
func (s *UserServer) ListUsers(
	ctx context.Context, req *pbUser.ListUsersRequest,
) (*pbUser.ListUsersResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]int32, 0, len(s.users))
	for id := range s.users {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	users := make([]*pbUser.User, 0, len(ids))
	for _, id := range ids {
		users = append(users, s.users[id])
	}
	return &pbUser.ListUsersResponse{Users: users}, nil
}

// ── GetUser ───────────────────────────────────────────────────────────────────
// Fetch full profile. Used by both user.get and user.get.active — the only
// difference between those two commands is the client-side completion filter;
// the RPC itself is identical.
func (s *UserServer) GetUser(
	ctx context.Context, req *pbUser.GetUserRequest,
) (*pbUser.GetUserResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	u, ok := s.users[req.UserId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "user %d not found", req.UserId)
	}
	return &pbUser.GetUserResponse{
		Id:        u.Id,
		Username:  u.Username,
		Email:     u.Email,
		Role:      u.Role,
		Active:    u.Active,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.CreatedAt, // simplified — same as created for demo
	}, nil
}

// GetActiveUser is routed to the same handler because the filtering is done
// entirely on the client side via filters: eq.  The server doesn't know or
// care — it's the same RPC response.
func (s *UserServer) GetActiveUser(
	ctx context.Context, req *pbUser.GetUserRequest,
) (*pbUser.GetUserResponse, error) {
	return s.GetUser(ctx, req)
}

// ── PromoteUser ───────────────────────────────────────────────────────────────
// Promote a user to admin role.
//
// The completion dropdown is pre-filtered on the client to show only
// role: editor | viewer (in_set filter) so the user never even sees
// existing admins as candidates.
func (s *UserServer) PromoteUser(
	ctx context.Context, req *pbUser.PromoteUserRequest,
) (*pbUser.PromoteUserResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[req.UserId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "user %d not found", req.UserId)
	}
	if u.Role == "admin" {
		return nil, status.Errorf(codes.AlreadyExists, "%s is already an admin", u.Username)
	}

	// Mutate a copy — keep the map consistent
	updated := *u
	updated.Role = "admin"
	s.users[req.UserId] = &updated

	// refresh_after_mutation: true on the proto annotation means the REPL
	// will automatically re-fetch ListUsers after this call returns,
	// so the completion dropdown reflects the role change immediately.
	return &pbUser.PromoteUserResponse{Success: true, Username: updated.Username}, nil
}

// ── ListUsersCmd ──────────────────────────────────────────────────────────────
// user.list — the human-facing list command (distinct from the bootstrap RPC
// which shares the same proto message but has no cmd annotation).
func (s *UserServer) ListUsersCmd(
	ctx context.Context, req *pbUser.ListUsersRequest,
) (*pbUser.ListUsersResponse, error) {
	return s.ListUsers(ctx, req)
}

// ----------------- Config Service ---------------

// ConfigServer implements pb.ConfigServiceServer.
type ConfigServer struct {
	pbConfig.UnimplementedConfigServiceServer

	mu sync.RWMutex
	// store["prod"]["database.url"] = "postgres://prod-db:5432/app"
	store map[string]map[string]string
	envs  []string
}

// NewConfigServer returns a ConfigServer seeded with demo data across three
// environments.
func NewConfigServer() *ConfigServer {
	s := &ConfigServer{
		store: map[string]map[string]string{
			"dev": {
				"database.url":       "postgres://localhost:5432/dev",
				"database.pool_size": "5",
				"feature.dark_mode":  "true",
				"feature.beta_ui":    "true",
				"log.level":          "debug",
			},
			"staging": {
				"database.url":       "postgres://staging-db:5432/app",
				"database.pool_size": "10",
				"feature.dark_mode":  "true",
				"feature.beta_ui":    "false",
				"log.level":          "info",
				"cache.ttl_seconds":  "300",
			},
			"prod": {
				"database.url":        "postgres://prod-db:5432/app",
				"database.pool_size":  "25",
				"feature.dark_mode":   "false",
				"feature.beta_ui":     "false",
				"log.level":           "warn",
				"cache.ttl_seconds":   "3600",
				"payment.gateway_url": "https://payments.example.com",
			},
		},
		envs: []string{"dev", "staging", "prod"},
	}
	return s
}

// ── ListEnvs ─────────────────────────────────────────────────────────────────
// Bootstrap RPC. Returns available environment names.
// Seeded into the --env completion dropdown on startup.
func (s *ConfigServer) ListEnvs(
	ctx context.Context, req *pbConfig.ListEnvsRequest,
) (*pbConfig.ListEnvsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &pbConfig.ListEnvsResponse{Envs: append([]string(nil), s.envs...)}, nil
}

// ── ListKeys ─────────────────────────────────────────────────────────────────
// Completion source for --key, parameterised by env.
//
// The REPL calls this via source_request: '{"env": "${ENV}"}' — meaning it
// sends whatever the user last `set ENV <value>` to.  The response contains
// ConfigEntry messages so the client-side "present" filter can inspect the
// "value" field to exclude tombstoned/empty entries from the dropdown.
//
// REPL flow:
//
//	> set ENV prod
//	> config.get --env prod --key <TAB>
//	(fetches ListKeys({"env":"prod"}) — only prod keys appear)
func (s *ConfigServer) ListKeys(
	ctx context.Context, req *pbConfig.ListKeysRequest,
) (*pbConfig.ListKeysResponse, error) {
	if req == nil || req.Env == "" {
		// Return all keys across all envs when env is unset (graceful fallback)
		s.mu.RLock()
		defer s.mu.RUnlock()
		seen := make(map[string]bool)
		for _, entries := range s.store {
			for k := range entries {
				seen[k] = true
			}
		}
		keys := make([]string, 0, len(seen))
		for k := range seen {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		entries := make([]*pbConfig.ConfigEntry, 0, len(keys))
		for _, k := range keys {
			entries = append(entries, &pbConfig.ConfigEntry{Key: k, Value: "?"})
		}
		return &pbConfig.ListKeysResponse{Keys: entries}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	envStore, ok := s.store[req.Env]
	if !ok {
		return &pbConfig.ListKeysResponse{}, nil
	}

	keys := make([]string, 0, len(envStore))
	for k := range envStore {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]*pbConfig.ConfigEntry, 0, len(keys))
	for _, k := range keys {
		// Include the value so the client-side "present" filter can work.
		entries = append(entries, &pbConfig.ConfigEntry{Key: k, Value: envStore[k]})
	}
	return &pbConfig.ListKeysResponse{Keys: entries}, nil
}

// ── GetConfig ─────────────────────────────────────────────────────────────────
// Fetch a single config entry.
// DISPLAY_KV + title_field:"env" means the REPL renders:
//
//	prod
//	─────────────────────────────────
//	key    database.url
//	value  postgres://prod-db:5432/app
func (s *ConfigServer) GetConfig(
	ctx context.Context, req *pbConfig.GetConfigRequest,
) (*pbConfig.GetConfigResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	envStore, ok := s.store[req.Env]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", req.Env)
	}
	value, ok := envStore[req.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %q not found in %q", req.Key, req.Env)
	}
	return &pbConfig.GetConfigResponse{Env: req.Env, Key: req.Key, Value: value}, nil
}

// ── SetConfig ─────────────────────────────────────────────────────────────────
// Write a config entry.
// success_message template: "✔ [{env}] {key} = {value}"
func (s *ConfigServer) SetConfig(
	ctx context.Context, req *pbConfig.SetConfigRequest,
) (*pbConfig.SetConfigResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.store[req.Env]; !ok {
		s.store[req.Env] = make(map[string]string)
		s.envs = append(s.envs, req.Env)
		sort.Strings(s.envs)
	}
	s.store[req.Env][req.Key] = req.Value
	return &pbConfig.SetConfigResponse{Success: true}, nil
}

// ── DumpConfig ────────────────────────────────────────────────────────────────
// DISPLAY_JSON: the entire response is rendered as an indented JSON block.
func (s *ConfigServer) DumpConfig(
	ctx context.Context, req *pbConfig.DumpConfigRequest,
) (*pbConfig.DumpConfigResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	envStore, ok := s.store[req.Env]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", req.Env)
	}

	entries := make(map[string]string, len(envStore))
	for k, v := range envStore {
		entries[k] = v
	}
	return &pbConfig.DumpConfigResponse{Env: req.Env, Entries: entries}, nil
}

// ExportConfig shares the same implementation as DumpConfig — only the REPL
// display mode differs (DISPLAY_RAW = compact single-line JSON).
func (s *ConfigServer) ExportConfig(
	ctx context.Context, req *pbConfig.DumpConfigRequest,
) (*pbConfig.DumpConfigResponse, error) {
	return s.DumpConfig(ctx, req)
}

// ----------------- Audit Service ----------------

// AuditServer implements pb.AuditServiceServer.
// The log is an append-only in-memory slice.  In production this would be a
// time-series DB or a Kafka consumer.
type AuditServer struct {
	pbAudit.UnimplementedAuditServiceServer

	mu     sync.RWMutex
	events []*pbAudit.AuditEvent
	byID   map[string]*pbAudit.AuditEvent
}

// NewAuditServer returns an AuditServer seeded with realistic demo events.
func NewAuditServer() *AuditServer {
	s := &AuditServer{
		byID: make(map[string]*pbAudit.AuditEvent),
	}

	now := time.Now().Unix()
	seed := []struct {
		id, actor, verb, target, svc, res string
		payload                           []byte
		meta                              map[string]string
		offset                            int64
	}{
		{"evt-001", "alice", "PUT", "/api/keys/foo", "kv.v1.KVService", "foo", []byte("alice"), map[string]string{"ip": "10.0.0.1"}, 600},
		{"evt-002", "bob", "GET", "/api/keys/foo", "kv.v1.KVService", "foo", []byte("bob"), map[string]string{"ip": "10.0.0.2"}, 540},
		{"evt-003", "SYSTEM", "CRON", "/jobs/gc", "scheduler.v1", "gc-job", []byte("system"), map[string]string{"job": "gc"}, 480},
		{"evt-004", "alice", "DELETE", "/api/keys/bar", "kv.v1.KVService", "bar", []byte("alice"), map[string]string{"ip": "10.0.0.1"}, 420},
		{"evt-005", "carol", "LOGIN", "/auth/login", "auth.v1.AuthService", "session", []byte("carol"), map[string]string{"ua": "curl"}, 360},
		{"evt-006", "bob", "PUT", "/api/keys/baz", "kv.v1.KVService", "baz", []byte("bob"), map[string]string{"ip": "10.0.0.2"}, 300},
		{"evt-007", "SYSTEM", "BACKUP", "/jobs/backup", "scheduler.v1", "bk-job", []byte("system"), map[string]string{"job": "backup"}, 240},
		{"evt-008", "alice", "PUT", "/config/prod/key", "config.v1", "prod/key", []byte("alice"), map[string]string{"ip": "10.0.0.1"}, 180},
	}

	for _, e := range seed {
		metaJSON, _ := json.Marshal(e.meta)
		evt := &pbAudit.AuditEvent{
			InternalId: e.id,
			Timestamp:  now - e.offset,
			Actor:      e.actor,
			Action:     &pbAudit.Action{Verb: e.verb, Target: e.target},
			Service:    e.svc,
			ResourceId: e.res,
			RawPayload: e.payload,
			Metadata:   string(metaJSON),
		}
		s.events = append(s.events, evt)
		s.byID[e.id] = evt
	}
	return s
}

// ── ListActors ────────────────────────────────────────────────────────────────
// Bootstrap RPC. Returns unique actors with their type classification.
//
// The neq filter on "type" in audit.stream.user will exclude entries where
// type == "system", so SYSTEM never appears in human-facing completions.
func (s *AuditServer) ListActors(
	ctx context.Context, req *pbAudit.ListActorsRequest,
) (*pbAudit.ListActorsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]string)
	for _, evt := range s.events {
		if _, ok := seen[evt.Actor]; !ok {
			t := "human"
			if evt.Actor == "SYSTEM" {
				t = "system"
			} else if evt.Service != "kv.v1.KVService" && evt.Service != "auth.v1.AuthService" {
				t = "service"
			}
			seen[evt.Actor] = t
		}
	}

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)

	actors := make([]*pbAudit.Actor, 0, len(names))
	for _, n := range names {
		actors = append(actors, &pbAudit.Actor{Name: n, Type: seen[n]})
	}
	return &pbAudit.ListActorsResponse{Actors: actors}, nil
}

// ── StreamEvents ─────────────────────────────────────────────────────────────
// Streams all historical events followed by new ones as they arrive.
//
// This demo replays the seeded events then enters a live ticker — in production
// you'd subscribe to an event bus.
//
// Column notes for the REPL rendering:
//   - timestamp   CELL_TIMESTAMP → "2024-11-14 14:02:11"
//   - raw_payload CELL_BYTES     → "61 6c 69 63 65"
//   - internal_id hidden:true    → not shown in stream columns
func (s *AuditServer) StreamEvents(
	req *pbAudit.StreamEventsRequest, stream pbAudit.AuditService_StreamEventsServer,
) error {
	s.mu.RLock()
	snapshot := make([]*pbAudit.AuditEvent, len(s.events))
	copy(snapshot, s.events)
	s.mu.RUnlock()

	for _, evt := range snapshot {
		if req.Service != "" && evt.Service != req.Service {
			continue
		}
		if err := stream.Send(evt); err != nil {
			return err
		}
	}

	// Simulate live events
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	n := 0
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case t := <-ticker.C:
			n++
			actors := []string{"alice", "bob", "carol", "SYSTEM"}
			actor := actors[n%len(actors)]
			evt := &pbAudit.AuditEvent{
				InternalId: fmt.Sprintf("evt-live-%04d", n),
				Timestamp:  t.Unix(),
				Actor:      actor,
				Action:     &pbAudit.Action{Verb: "PING", Target: "/health"},
				Service:    "health.v1",
				ResourceId: "health",
				RawPayload: []byte(actor),
				Metadata:   `{"live":true}`,
			}
			if req.Service != "" && evt.Service != req.Service {
				continue
			}
			if err := stream.Send(evt); err != nil {
				return err
			}
		}
	}
}

// ── StreamUserEvents ─────────────────────────────────────────────────────────
// Stream events for a specific actor.  The neq filter on the completion source
// means SYSTEM is not offered as a candidate — but if someone types it
// manually the server will still filter here too.
func (s *AuditServer) StreamUserEvents(
	req *pbAudit.StreamEventsRequest, stream pbAudit.AuditService_StreamUserEventsServer,
) error {
	s.mu.RLock()
	snapshot := make([]*pbAudit.AuditEvent, len(s.events))
	copy(snapshot, s.events)
	s.mu.RUnlock()

	for _, evt := range snapshot {
		if evt.Actor == "SYSTEM" {
			continue // defensive: server-side mirror of client-side neq filter
		}
		if req.Actor != "" && evt.Actor != req.Actor {
			continue
		}
		if err := stream.Send(evt); err != nil {
			return err
		}
	}
	// For brevity, no live simulation in the user stream.
	return nil
}

// ── GetEvent ─────────────────────────────────────────────────────────────────
// Fetch a single event by internal ID.
//
// Rendered in DISPLAY_KV + expand_nested:true, so the Action sub-message
// is inlined:
//
//	internal_id  evt-004
//	time         2024-11-14 14:02:34
//	actor        alice
//	action.verb  DELETE
//	action.target /api/keys/bar
//	service      kv.v1.KVService
//	resource     bar
//	payload      61 6c 69 63 65
//	metadata     {"ip":"10.0.0.1"}
func (s *AuditServer) GetEvent(
	ctx context.Context, req *pbAudit.GetEventRequest,
) (*pbAudit.AuditEvent, error) {
	if req == nil || req.EventId == "" {
		return nil, status.Error(codes.InvalidArgument, "event_id required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	evt, ok := s.byID[req.EventId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "event %q not found", req.EventId)
	}
	return evt, nil
}

// ── SearchEvents ─────────────────────────────────────────────────────────────
// Search by actor. The metadata column uses CELL_JSON for inline rendering.
func (s *AuditServer) SearchEvents(
	ctx context.Context, req *pbAudit.SearchEventsRequest,
) (*pbAudit.SearchEventsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*pbAudit.AuditEvent
	for _, evt := range s.events {
		if req.Actor != "" && evt.Actor != req.Actor {
			continue
		}
		results = append(results, evt)
		if len(results) >= limit {
			break
		}
	}
	return &pbAudit.SearchEventsResponse{Events: results}, nil
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

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(chainUnaryInterceptors(
			unaryAuthInterceptor(authManager, methodAction),
			unaryLoggingInterceptor,
		)),
		grpc.StreamInterceptor(chainStreamInterceptors(
			streamAuthInterceptor(authManager, methodAction),
			streamLoggingInterceptor,
		)),
	)

	pbKV.RegisterKVServiceServer(grpcServer, NewKVServer())
	pbAudit.RegisterAuditServiceServer(grpcServer, NewAuditServer())
	pbConfig.RegisterConfigServiceServer(grpcServer, NewConfigServer())
	pbUser.RegisterUserServiceServer(grpcServer, NewUserServer())

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
