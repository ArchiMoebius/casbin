package auth

import (
	"crypto/rsa"
	"errors"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	jwt.RegisteredClaims
}

// GenerateJWT generates a signed JWT with the correct kid
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

// ValidateToken validates a JWT against a map of public keys
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

// MustLoadPublicKey loads RSA public key or panics
func MustLoadPublicKey(path string) *rsa.PublicKey {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	key, err := jwt.ParseRSAPublicKeyFromPEM(data)
	if err != nil {
		panic(err)
	}
	return key
}

// MustLoadPrivateKey loads RSA private key or panics
func MustLoadPrivateKey(path string) *rsa.PrivateKey {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	key, err := jwt.ParseRSAPrivateKeyFromPEM(data)
	if err != nil {
		panic(err)
	}
	return key
}
