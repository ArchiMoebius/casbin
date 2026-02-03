#!/bin/bash
# Generate RSA key pair for JWT signing

set -e

echo "Generating RSA key pair for JWT..."

# Create config directory if it doesn't exist
mkdir -p config

# Generate private key (2048-bit RSA)
echo "Generating private key..."
openssl genrsa -out config/private_key.pem 2048

# Generate public key from private key
echo "Generating public key..."
openssl rsa -in config/private_key.pem -pubout -out config/public_key.pem

# Set appropriate permissions
chmod 600 config/private_key.pem
chmod 644 config/public_key.pem

echo "✅ Keys generated successfully in config/ directory:"
echo "   - config/private_key.pem (keep this secret!)"
echo "   - config/public_key.pem"
