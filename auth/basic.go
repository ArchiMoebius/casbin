package auth

import (
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/casbin/casbin/v2"
)

// ----------------- Auth Manager -----------------

// AuthManager holds the Casbin enforcer, the credential store for Basic auth,
// and the public key store for JWT auth. It is safe for concurrent use.
type AuthManager struct {
	mu       sync.RWMutex
	enforcer *casbin.Enforcer
	users    map[string]string         // username -> password (Basic auth)
	jwtKeys  map[string]*rsa.PublicKey // kid -> RSA public key (JWT auth)
}

// NewAuthManager loads the Casbin model and policy from disk and returns
// a ready-to-use AuthManager. users is the credential map for Basic auth;
// jwtKeys is the public key map for JWT validation. Neither map is copied,
// so the caller should not mutate them after this call. Either map can be
// nil if that auth method is not in use.
func NewAuthManager(modelPath, policyPath string, users map[string]string, jwtKeys map[string]*rsa.PublicKey) (*AuthManager, error) {
	enforcer, err := casbin.NewEnforcer(modelPath, policyPath)
	if err != nil {
		return nil, err
	}
	return &AuthManager{
		enforcer: enforcer,
		users:    users,
		jwtKeys:  jwtKeys,
	}, nil
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

// ValidateUser auto-detects the auth scheme from the Authorization header
// and delegates to the appropriate validator. Supports:
//   - "Basic ..." → validateBasic
//   - "Bearer ..." → ValidateJWT
//
// Returns the username/userID on success. All errors are plain fmt errors —
// the caller is responsible for mapping them to the appropriate transport
// status code.
func (am *AuthManager) ValidateUser(authHeader string) (string, error) {
	if authHeader == "" {
		return "", fmt.Errorf("missing authorization header")
	}

	// Auto-detect scheme
	if strings.HasPrefix(authHeader, "Basic ") {
		return am.validateBasic(authHeader)
	}
	if strings.HasPrefix(authHeader, "Bearer ") {
		return am.ValidateJWT(authHeader)
	}

	return "", fmt.Errorf("unsupported authorization scheme")
}

// validateBasic handles "Basic ..." credentials.
func (am *AuthManager) validateBasic(authHeader string) (string, error) {
	payload, err := base64.StdEncoding.DecodeString(authHeader[len("Basic "):])
	if err != nil {
		return "", fmt.Errorf("invalid base64: %w", err)
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid basic auth format")
	}
	username, password := parts[0], parts[1]

	if am.users == nil {
		return "", fmt.Errorf("basic auth not configured")
	}

	stored, ok := am.users[username]
	if !ok || subtle.ConstantTimeCompare([]byte(stored), []byte(password)) != 1 {
		return "", fmt.Errorf("invalid credentials")
	}

	return username, nil
}

// ValidateJWT handles "Bearer ..." tokens. Validates the JWT signature
// against the configured public keys and returns the UserID claim.
func (am *AuthManager) ValidateJWT(authHeader string) (string, error) {
	tokenStr := authHeader[len("Bearer "):]

	if am.jwtKeys == nil {
		return "", fmt.Errorf("JWT auth not configured")
	}

	claims, err := ValidateToken(tokenStr, am.jwtKeys)
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}

	if claims.UserID == "" {
		return "", fmt.Errorf("missing user_id claim")
	}

	return claims.UserID, nil
}

// ----------------- Authorization -----------------

// Authorize checks whether userID is allowed to perform action on obj
// according to the current Casbin policy.
func (am *AuthManager) Authorize(userID, obj, action string) (bool, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.enforcer.Enforce(userID, obj, action)
}
