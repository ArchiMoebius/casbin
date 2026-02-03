package auth

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/casbin/casbin/v2"
)

// ----------------- Auth Manager -----------------

// AuthManager holds the Casbin enforcer and the credential store.
// It is safe for concurrent use.
type AuthManager struct {
	mu       sync.RWMutex
	enforcer *casbin.Enforcer
	users    map[string]string // username -> password
}

// NewAuthManager loads the Casbin model and policy from disk and returns
// a ready-to-use AuthManager. users is the credential map; it is not
// copied, so the caller should not mutate it after this call.
func NewAuthManager(modelPath, policyPath string, users map[string]string) (*AuthManager, error) {
	enforcer, err := casbin.NewEnforcer(modelPath, policyPath)
	if err != nil {
		return nil, err
	}
	return &AuthManager{enforcer: enforcer, users: users}, nil
}

// ReloadPolicies re-reads the policy CSV into the existing enforcer.
// Safe to call from a signal handler goroutine.
func (am *AuthManager) ReloadPolicies() {
	am.mu.Lock()
	defer am.mu.Unlock()

	if err := am.enforcer.LoadPolicy(); err != nil {
		log.Printf("❌ Failed to reload policies: %v", err)
		return
	}
	log.Println("✅ Casbin policies reloaded successfully")
}

// ----------------- Authentication -----------------

// ValidateUser extracts and verifies the credential from a raw
// "Basic ..." Authorization header value.  Returns the username on
// success.  All errors are plain fmt errors — the caller is responsible
// for mapping them to the appropriate transport status code.
func (am *AuthManager) ValidateUser(authHeader string) (string, error) {
	if authHeader == "" || !strings.HasPrefix(authHeader, "Basic ") {
		return "", fmt.Errorf("missing or invalid authorization header")
	}

	payload, err := base64.StdEncoding.DecodeString(authHeader[len("Basic "):])
	if err != nil {
		return "", fmt.Errorf("invalid base64: %w", err)
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid basic auth format")
	}
	username, password := parts[0], parts[1]

	stored, ok := am.users[username]
	if !ok || subtle.ConstantTimeCompare([]byte(stored), []byte(password)) != 1 {
		return "", fmt.Errorf("invalid credentials")
	}

	return username, nil
}

// ----------------- Authorization -----------------

// Authorize checks whether userID is allowed to perform action on obj
// according to the current Casbin policy.
func (am *AuthManager) Authorize(userID, obj, action string) (bool, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.enforcer.Enforce(userID, obj, action)
}
