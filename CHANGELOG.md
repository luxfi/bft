# Changelog

All notable changes to the Lux BFT consensus implementation will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.1] - 2025-11-11

### Changed
- **[BREAKING]** Migrated logging from `go.uber.org/zap` to `github.com/luxfi/log`
  - All production code now uses `github.com/luxfi/log` package
  - Updated Logger interface to use `log.Field` instead of `zap.Field`
  - Updated all field constructors (`String`, `Int`, `Uint64`, `Duration`, `Time`, etc.)
  - Test infrastructure maintains zap wrapper for compatibility
  - Log format changed to luxfi/log style: `[MM-DD|HH:MM:SS.mmm] LEVEL file:line message`

### Added
- Created `version.go` with version constants
- Added CHANGELOG.md for tracking releases
- Ported missing test cases from upstream simplex implementation:
  - TestEpochVotesForEquivocatedVotes (epoch_test.go)
  - TestBlockNotVerifiedIfParentNotNotarized (epoch_test.go)
  - TestReplicationStuckInProposingBlock (replication_test.go)
  - TestReplicationVerifyEmptyNotarization (replication_test.go)
  - TestReplicationVerifyNotarization (replication_test.go)
  - TestReplicationMalformedQuorumRound (replication_timeout_test.go)
  - TestReplicationResendsFinalizedBlocksThatFailedVerification (replication_timeout_test.go)

### Fixed
- Fixed nil pointer dereference in `verifyEmptyNotarization` by adding QC nil check
- Fixed circular import in `sched_test.go` by implementing inline noOpLogger
- Enhanced `recordingComm` with `Send()` method and `SentMessages` channel
- Added `verificationError` field to `testBlock` for error injection in tests
- Fixed compilation errors in epoch, replication, and timeout test files

## [v0.1.0] - 2025-11-10

### Added
- Initial BFT consensus implementation
- Epoch management with leader rotation
- Quorum certificate (QC) aggregation
- Notarization and finalization protocols
- Empty vote mechanism for timeout handling
- Blacklist mechanism using orbital suspicion tracking
- WAL (Write-Ahead Log) for crash recovery
- Block replication and synchronization
- Comprehensive test suite
