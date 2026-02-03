.PHONY: all proto keys config run client test clean help

# Default target
all: config proto run

# Generate protobuf code
proto:
	@echo "📦 Generating protobuf code..."
	@mkdir -p gen/go/kv/v1
	@protoc --go_out=gen/go --go_opt=paths=source_relative \
		--go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
		proto/kv/v1/kv.proto
	@echo "✅ Protobuf code generated"

# Generate RSA keys
keys:
	@echo "🔑 Generating RSA keys..."
	@bash generate_keys.sh

# Create config files
config: keys
	@echo "📝 Creating config files..."
	@mkdir -p config
	@if [ ! -f config/model.conf ]; then \
		echo "Creating model.conf..."; \
		echo "[request_definition]" > config/model.conf; \
		echo "r = sub, obj, act" >> config/model.conf; \
		echo "" >> config/model.conf; \
		echo "[policy_definition]" >> config/model.conf; \
		echo "p = sub, obj, act" >> config/model.conf; \
		echo "" >> config/model.conf; \
		echo "[role_definition]" >> config/model.conf; \
		echo "g = _, _" >> config/model.conf; \
		echo "" >> config/model.conf; \
		echo "[policy_effect]" >> config/model.conf; \
		echo "e = some(where (p.eft == allow))" >> config/model.conf; \
		echo "" >> config/model.conf; \
		echo "[matchers]" >> config/model.conf; \
		echo "m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act" >> config/model.conf; \
	fi
	@if [ ! -f config/policy.csv ]; then \
		echo "Creating policy.csv..."; \
		echo "p, admin, kv, get" > config/policy.csv; \
		echo "p, admin, kv, put" >> config/policy.csv; \
		echo "p, reader, kv, get" >> config/policy.csv; \
		echo "g, user-alice-123, admin" >> config/policy.csv; \
		echo "g, user-bob-456, reader" >> config/policy.csv; \
	fi
	@echo "✅ Config files created"

# Install dependencies
deps:
	@echo "📥 Installing dependencies..."
	@go mod download
	@go mod tidy
	@echo "✅ Dependencies installed"

# Build server binary
build: proto
	@echo "🔨 Building server..."
	@go build -o bin/server main.go
	@echo "✅ Server built: bin/server"

# Build client binary
build-client: proto
	@echo "🔨 Building client..."
	@go build -o bin/client client_example.go
	@echo "✅ Client built: bin/client"

# Run server
run: proto
	@echo "🚀 Starting server..."
	@go run main.go

# Run client example
client: proto
	@echo "🧪 Running client example..."
	@go run client_example.go

# Run tests
test:
	@echo "🧪 Running tests..."
	@go test -v ./...

# Clean generated files
clean:
	@echo "🧹 Cleaning up..."
	@rm -rf gen/
	@rm -rf bin/
	@rm -rf config/private_key.pem config/public_key.pem
	@echo "✅ Cleaned"

# Clean everything including config
clean-all: clean
	@echo "🧹 Cleaning all config files..."
	@rm -rf config/
	@echo "✅ All cleaned"

# Help target
help:
	@echo "Available targets:"
	@echo "  all          - Setup config, generate proto, and run server (default)"
	@echo "  proto        - Generate protobuf code"
	@echo "  keys         - Generate RSA key pair"
	@echo "  config       - Create config files (model.conf, policy.csv) and keys"
	@echo "  deps         - Install Go dependencies"
	@echo "  build        - Build server binary"
	@echo "  build-client - Build client binary"
	@echo "  run          - Run server"
	@echo "  client       - Run client example"
	@echo "  test         - Run tests"
	@echo "  clean        - Remove generated files"
	@echo "  clean-all    - Remove all generated files including config"
	@echo "  help         - Show this help message"
