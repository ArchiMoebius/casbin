PROTO_DIR := proto
GO_OUT_DIR := pkg/gen
PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto" ! -path "*/google/*")

GOPATH_SRC := $(if $(GOPATH),$(GOPATH)/src,)
PROTO_INCLUDES := -I$(PROTO_DIR) $(if $(GOPATH_SRC),-I$(GOPATH_SRC)) -I/usr/local/include -I/usr/include -I.

BUILD=$(shell git rev-parse HEAD)
BASEDIR=./dist
DIR=${BASEDIR}/temp

BUILD_UUID=$(shell cat /proc/sys/kernel/random/uuid)

LDFLAGS=-ldflags "-s -w -X 'main.build=${BUILD}' -X 'github.com/archimoebius/fishler/app.ServiceUUIDString=${BUILD_UUID}' -buildid=${BUILD}"
GCFLAGS=-gcflags=all=-trimpath=$(shell pwd)
ASMFLAGS=-asmflags=all=-trimpath=$(shell pwd)

GOFILES=`go list -buildvcs=false ./...`
GOFILESNOTEST=`go list -buildvcs=false ./... | grep -v test`

# Make Directory to store executables
$(shell mkdir -p ${DIR})


PROTOC := protoc
PROTOC_GEN_GO := protoc-gen-go
PROTOC_GEN_GO_GRPC := protoc-gen-go-grpc
PROTOC_VERSION := latest
PROTOC_GEN_GO_VERSION := latest
PROTOC_GEN_GO_GRPC_VERSION := latest

COLOR_RESET := \033[0m
COLOR_BLUE := \033[34m
COLOR_GREEN := \033[32m
COLOR_YELLOW := \033[33m

.PHONY: all
all: check-tools generate linux

.PHONY: help
help:
	@echo "$(COLOR_BLUE)Available targets:$(COLOR_RESET)"
	@echo "  $(COLOR_GREEN)all$(COLOR_RESET)           - Check tools and generate Go code"
	@echo "  $(COLOR_GREEN)generate$(COLOR_RESET)      - Generate Go code from proto files"
	@echo "  $(COLOR_GREEN)check-tools$(COLOR_RESET)   - Verify required tools are installed"
	@echo "  $(COLOR_GREEN)install-tools$(COLOR_RESET) - Install required protoc plugins"
	@echo "  $(COLOR_GREEN)clean$(COLOR_RESET)         - Remove generated files"
	@echo "  $(COLOR_GREEN)deps$(COLOR_RESET)          - Download googleapis dependencies"
	@echo "  $(COLOR_GREEN)verify$(COLOR_RESET)        - Verify generated code compiles"

.PHONY: check-tools
check-tools:
	@echo "$(COLOR_BLUE)Checking required tools...$(COLOR_RESET)"
	@command -v $(PROTOC) >/dev/null 2>&1 || { echo "$(COLOR_YELLOW)protoc not found. Please install Protocol Buffers compiler from https://github.com/protocolbuffers/protobuf/releases/$(COLOR_RESET)"; exit 1; }
	@command -v $(PROTOC_GEN_GO) >/dev/null 2>&1 || { echo "$(COLOR_YELLOW)protoc-gen-go not found. Run 'make install-tools'$(COLOR_RESET)"; exit 1; }
	@command -v $(PROTOC_GEN_GO_GRPC) >/dev/null 2>&1 || { echo "$(COLOR_YELLOW)protoc-gen-go-grpc not found. Run 'make install-tools'$(COLOR_RESET)"; exit 1; }
	@echo "$(COLOR_GREEN)✓ All tools installed$(COLOR_RESET)"

.PHONY: install-tools
install-tools:
	@echo "$(COLOR_BLUE)Installing protoc-gen-go...$(COLOR_RESET)"
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@echo "$(COLOR_BLUE)Installing protoc-gen-go-grpc...$(COLOR_RESET)"
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@echo "$(COLOR_GREEN)✓ Tools installed$(COLOR_RESET)"

.PHONY: deps
deps:
	@echo "$(COLOR_BLUE)Downloading googleapis dependencies...$(COLOR_RESET)"
	@mkdir -p $(PROTO_DIR)/google/api
	@curl -sL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/field_info.proto \
		-o $(PROTO_DIR)/google/api/field_info.proto
	@echo "$(COLOR_GREEN)✓ Dependencies downloaded$(COLOR_RESET)"

.PHONY: generate
generate: check-tools
	@echo "$(COLOR_BLUE)Generating Go code from proto files...$(COLOR_RESET)"
	@mkdir -p $(GO_OUT_DIR)
	@for proto in $(PROTO_FILES); do \
		echo "  Processing $$proto..."; \
		$(PROTOC) $(PROTO_INCLUDES) \
			--go_out=$(GO_OUT_DIR) \
			--go_opt=paths=source_relative \
			--go-grpc_out=$(GO_OUT_DIR) \
			--go-grpc_opt=paths=source_relative \
			$$proto || exit 1; \
	done
	@echo "$(COLOR_GREEN)✓ Code generation complete$(COLOR_RESET)"

.PHONY: generate-with-grpc
generate-with-grpc: check-tools
	@echo "$(COLOR_BLUE)Generating Go code with gRPC support...$(COLOR_RESET)"
	@mkdir -p $(GO_OUT_DIR)
	@for proto in $(PROTO_FILES); do \
		echo "  Processing $proto..."; \
		$(PROTOC) $(PROTO_INCLUDES) \
			--go_out=$(GO_OUT_DIR) \
			--go_opt=paths=source_relative \
			--go-grpc_out=$(GO_OUT_DIR) \
			--go-grpc_opt=paths=source_relative \
			$proto || exit 1; \
	done
	@echo "$(COLOR_GREEN)✓ Code generation complete$(COLOR_RESET)"

.PHONY: clean
clean:
	@echo "$(COLOR_BLUE)Cleaning generated files...$(COLOR_RESET)"
	@rm -rf $(GO_OUT_DIR)
	@echo "$(COLOR_GREEN)✓ Clean complete$(COLOR_RESET)"

.PHONY: verify
verify: generate
	@echo "$(COLOR_BLUE)Verifying generated code compiles...$(COLOR_RESET)"
	@cd $(GO_OUT_DIR) && go build ./... || exit 1
	@echo "$(COLOR_GREEN)✓ Generated code verified$(COLOR_RESET)"

.PHONY: format
format:
	@echo "$(COLOR_BLUE)Formatting proto files...$(COLOR_RESET)"
	@command -v clang-format >/dev/null 2>&1 && \
		find $(PROTO_DIR) -name "*.proto" -exec clang-format -i {} \; || \
		echo "$(COLOR_YELLOW)clang-format not found, skipping format$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)✓ Format complete$(COLOR_RESET)"

linux:
	@env CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath ${LDFLAGS} ${GCFLAGS} ${ASMFLAGS} -o ${DIR}/casbin-server-linux_amd64 main.go
