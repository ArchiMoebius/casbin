package auth

import (
	"encoding/base64"
	"fmt"
	"strings"

	"log"
	"sync"

	"github.com/casbin/casbin/v2"
)

// ----------------- USERS -----------------
var users = map[string]string{
	"alice": "alice123",
	"bob":   "bob123",
}

// ----------------- AUTH MANAGER -----------------
type AuthManager struct {
	mu       sync.RWMutex
	enforcer *casbin.Enforcer
}

// NewAuthManager loads Casbin model/policy
func NewAuthManager(modelPath, policyPath string) (*AuthManager, error) {
	enf, err := casbin.NewEnforcer(modelPath, policyPath)
	if err != nil {
		return nil, err
	}
	return &AuthManager{enforcer: enf}, nil
}

// ReloadPolicies reloads Casbin policy from disk
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

// ----------------- AUTHENTICATION -----------------
func ValidateBasicAuth(authHeader string) (string, error) {
	if authHeader == "" || !strings.HasPrefix(authHeader, "Basic ") {
		return "", fmt.Errorf("missing or invalid Authorization header")
	}

	payload, err := base64.StdEncoding.DecodeString(authHeader[len("Basic "):])
	if err != nil {
		return "", fmt.Errorf("invalid base64 encoding: %w", err)
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid auth format")
	}

	username, password := parts[0], parts[1]
	if users[username] != password {
		return "", fmt.Errorf("invalid credentials")
	}

	return username, nil
}

// ----------------- AUTHORIZATION -----------------
func (am *AuthManager) Authorize(userID, obj, action string) (bool, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.enforcer.Enforce(userID, obj, action)
}
