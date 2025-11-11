# AI Assistant Knowledge Base

**Last Updated**: 2025-11-11
**Project**: BFT Consensus Engine
**Organization**: Lux Industries, Inc.
**Version**: v0.1.1

## Project Overview

This repository contains the Byzantine Fault Tolerant (BFT) consensus implementation for the Lux blockchain. It provides a standalone, reusable consensus protocol with features including:

- **Epoch-based Consensus**: Validator rotation and reconfiguration
- **Quorum Certificates (QC)**: Cryptographic proof of validator agreement
- **Leader Rotation**: Round-robin block proposal mechanism
- **Notarization & Finalization**: Two-phase commit protocol
- **Empty Vote Mechanism**: Timeout handling for liveness
- **Blacklist System**: Orbital suspicion tracking for Byzantine nodes
- **WAL Support**: Write-ahead logging for crash recovery
- **BLS Signatures**: Efficient signature aggregation

## Essential Commands

### Development
```bash
# Build all packages
go build ./...

# Run all tests
go test -v ./...

# Run specific test patterns
go test -v -run TestBlacklist
go test -v -run TestEpoch

# Run tests with timeout
go test -v -timeout 30s ./...

# Run tests with race detector
go test -race ./...

# Check test coverage
go test -cover ./...
```

## Architecture

### Core Components

1. **Epoch** (`epoch.go`): Main consensus coordinator managing rounds, votes, and state transitions
2. **Notarization** (`notarization.go`): Vote collection and QC aggregation
3. **Replication** (`replication.go`): Block synchronization and retrieval
4. **Monitor** (`monitor.go`): Asynchronous task scheduling and timeouts
5. **Scheduler** (`sched.go`): Dependency-based task execution
6. **Blacklist** (`blacklist.go`): Byzantine node detection using orbits
7. **Timeout Handler** (`timeout_handler.go`): Progress monitoring and round advancement

### Interfaces

- **Logger**: Structured logging with Fatal/Error/Warn/Info/Trace/Debug/Verbo levels
- **BlockBuilder**: Application-specific block creation
- **Storage**: Persistent block and finalization storage
- **Communication**: P2P message broadcasting and unicast
- **Signer**: BLS signature creation
- **SignatureVerifier**: BLS signature validation
- **SignatureAggregator**: QC generation from individual signatures
- **WriteAheadLog**: Crash recovery support

## Key Technologies

- **Go 1.25.1**: Primary implementation language
- **github.com/luxfi/log**: Structured logging (migrated from zap)
- **BLS Signatures**: Cryptographic primitives for consensus
- **Write-Ahead Logging**: Durability and crash recovery

## Development Workflow

### Testing Strategy
- Unit tests for core logic (epoch, notarization, replication)
- Integration tests for multi-node scenarios
- Table-driven tests for blacklist logic
- Mock implementations for testing (testutil package)

### Recent Changes (v0.1.1)
- Migrated from `go.uber.org/zap` to `github.com/luxfi/log`
- Ported additional test cases from upstream
- Fixed nil pointer bugs and circular imports
- Enhanced test infrastructure

## Context for All AI Assistants

This file (`LLM.md`) is symlinked as:
- `.AGENTS.md`
- `CLAUDE.md`
- `QWEN.md`
- `GEMINI.md`

All files reference the same knowledge base. Updates here propagate to all AI systems.

## Rules for AI Assistants

1. **ALWAYS** update LLM.md with significant discoveries
2. **NEVER** commit symlinked files (.AGENTS.md, CLAUDE.md, etc.) - they're in .gitignore
3. **NEVER** create random summary files - update THIS file

---

**Note**: This file serves as the single source of truth for all AI assistants working on this project.
