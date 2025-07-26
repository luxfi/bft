# BFT Makefile
# Build, test, and manage the Lux BFT implementation

.PHONY: all build test clean lint fmt vet coverage bench help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Build parameters
BINARY_NAME=bft
BUILD_DIR=build
COVERAGE_FILE=coverage.out
COVERAGE_HTML=coverage.html

# Default target
all: clean lint vet test build

# Help target
help:
	@echo "Available targets:"
	@echo "  make build      - Build the project"
	@echo "  make test       - Run all tests"
	@echo "  make coverage   - Run tests with coverage report"
	@echo "  make bench      - Run benchmarks"
	@echo "  make lint       - Run golint"
	@echo "  make fmt        - Format code with gofmt"
	@echo "  make vet        - Run go vet"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make deps       - Download dependencies"
	@echo "  make tidy       - Tidy go.mod"
	@echo "  make all        - Run clean, lint, vet, test, and build"

# Build the project (library - just compile check)
build:
	@echo "Building..."
	$(GOBUILD) -v ./...

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race -timeout 30s ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	$(GOCMD) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

# Run golint (requires golint to be installed)
lint:
	@echo "Running lint..."
	@if command -v golint > /dev/null; then \
		golint ./...; \
	else \
		echo "golint not installed. Run: go install golang.org/x/lint/golint@latest"; \
	fi

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

# Run go vet
vet:
	@echo "Running vet..."
	$(GOVET) ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f $(COVERAGE_FILE) $(COVERAGE_HTML)
	$(GOCLEAN)

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download

# Tidy go.mod
tidy:
	@echo "Tidying go.mod..."
	$(GOMOD) tidy

# Run tests in verbose mode with specific package
test-verbose:
	$(GOTEST) -v -count=1 ./...

# Run a specific test
test-specific:
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make test-specific TEST=TestName"; \
		exit 1; \
	fi
	$(GOTEST) -v -run $(TEST) ./...

# Check for security vulnerabilities
sec:
	@echo "Checking for vulnerabilities..."
	@if command -v gosec > /dev/null; then \
		gosec -quiet ./...; \
	else \
		echo "gosec not installed. Run: go install github.com/securego/gosec/v2/cmd/gosec@latest"; \
	fi

# Generate test coverage badge (requires gocov-badge)
badge:
	@if command -v gocov-badge > /dev/null; then \
		$(GOTEST) -coverprofile=$(COVERAGE_FILE) ./... && \
		gocov-badge -in=$(COVERAGE_FILE) -out=coverage.svg; \
	else \
		echo "gocov-badge not installed. Run: go install github.com/jpoles1/gocov-badge@latest"; \
	fi

# Quick test (no race detector, faster)
test-quick:
	$(GOTEST) ./...

# Install development tools
install-tools:
	@echo "Installing development tools..."
	go install golang.org/x/lint/golint@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install github.com/jpoles1/gocov-badge@latest