package auth

import (
	"crypto/rsa"
	"errors"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims represents the JWT claims structure for this service.
type Claims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	jwt.RegisteredClaims
}

// GenerateJWT generates a signed JWT with the specified kid (key ID).
// The token is valid for 24 hours from issuance.
func GenerateJWT(userID, email string, privateKey *rsa.PrivateKey, kid string) (string, error) {
	claims := &Claims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "kv-service",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	return token.SignedString(privateKey)
}

// ValidateToken validates a JWT against a map of public keys indexed by kid.
// The kid is extracted from the token header and used to select the
// appropriate public key for signature verification.
func ValidateToken(tokenStr string, pubKeys map[string]*rsa.PublicKey) (*Claims, error) {
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	token, _, err := parser.ParseUnverified(tokenStr, &Claims{})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, errors.New("invalid claims type")
	}

	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return nil, errors.New("missing kid")
	}

	pubKey, ok := pubKeys[kid]
	if !ok {
		return nil, errors.New("unknown kid")
	}

	token, err = jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return pubKey, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok = token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	return claims, nil
}

// LoadPublicKey loads an RSA public key from a PEM file.
// Returns an error if the file doesn't exist or is invalid.
// Use this when the key is optional or you want to handle errors explicitly.
func LoadPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := jwt.ParseRSAPublicKeyFromPEM(data)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// MustLoadPublicKey loads an RSA public key from a PEM file or panics.
// Use this for required keys at startup.
func MustLoadPublicKey(path string) *rsa.PublicKey {
	key, err := LoadPublicKey(path)
	if err != nil {
		panic(err)
	}
	return key
}

// LoadPrivateKey loads an RSA private key from a PEM file.
// Returns an error if the file doesn't exist or is invalid.
// Use this when the key is optional or you want to handle errors explicitly.
func LoadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := jwt.ParseRSAPrivateKeyFromPEM(data)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// MustLoadPrivateKey loads an RSA private key from a PEM file or panics.
// Use this for required keys at startup.
func MustLoadPrivateKey(path string) *rsa.PrivateKey {
	key, err := LoadPrivateKey(path)
	if err != nil {
		panic(err)
	}
	return key
}
