# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

s3utils is a collection of utilities and scripts for managing S3-compatible storage at scale, specifically designed for Scality S3 Connector (S3C) and Zenko deployments. The project provides operational tools for Cross-Region Replication (CRR), metadata verification, Raft consensus debugging, and data integrity checks.

## Common Commands

### Testing

```bash
# Run unit tests
yarn test:unit

# Run functional tests (requires MongoDB and workbench environment)
# With workbench installed, use: workbench up  --env-dir ./workbench/env -d
yarn test:functional

# Run both test suites with coverage
yarn test:unit && yarn test:functional
```

### Linting

```bash
# Run ESLint on tracked files
yarn lint

# Lint with zero warnings (CI mode)
yarn lint -- --max-warnings 0
```

### Running Scripts

Most scripts are executed directly with Node.js and require environment variables for configuration:

```bash
# Example: CRR existing objects
node crrExistingObjects.js bucket1,bucket2

# Example: Verify bucket sproxyd keys
node verifyBucketSproxydKeys.js

# Example: Cleanup noncurrent versions
node cleanupNoncurrentVersions.js bucket1,bucket2
```

Many scripts are also designed to run inside Docker containers with proper network and volume mounts (see README.md for specific examples).

## Architecture Overview

### Core Component Structure

**Script Categories:**
- **CRR Management** (`CRR/`, `crrExistingObjects.js`, `autoRetryFailedCRR.js`, `requeueFailedCRRCronJob.js`): Cross-Region Replication utilities for triggering and managing replication of existing objects
- **Metadata Verification** (`verifyBucketSproxydKeys.js`, `VerifyBucketSproxydKeys/`): Validates sproxyd keys referenced in S3 metadata exist on storage backend
- **Raft Consensus Tools** (`CompareRaftMembers/`): Compares metadata databases between Raft leader and followers to detect and repair divergences
- **Data Cleanup** (`cleanupBuckets.js`, `cleanupNoncurrentVersions.js`, `removeDeleteMarkers.js`): Utilities for managing object versions and lifecycle
- **Replication Verification** (`VerifyReplication/`): Compares source and destination buckets to verify replication completeness
- **Object Repair** (`ObjectRepair/`, `repairDuplicateVersions*.js`): Repairs metadata inconsistencies and duplicate keys
- **Monitoring & Reporting** (`service-level-sidecar/`, `DataReport/`, `bucketVersionsStats.js`): UtapiV2 service-level metrics and bucket statistics
- **Utilities** (`Clients/`, `utils/`): Shared S3 and CloudServer client wrappers

### Key Architectural Patterns

**Client Architecture:**
- `Clients/s3Client.js`: AWS SDK v3 S3 client with custom retry strategy (100 retries with exponential backoff)
- `Clients/CloudserverClient.js`: Internal CloudServer API client for direct metadata operations
- Both clients use configurable retry logic and keep-alive connections for high-throughput operations

**Replication Status Management:**
- `CRR/ReplicationStatusUpdater.js`: Core class for updating object replication metadata
- Processes objects in batches with configurable worker concurrency
- Supports filtering by replication status: NEW, PENDING, COMPLETED, FAILED, REPLICA
- Can target specific prefixes and resume from markers for large-scale operations

**Metadata Database Tools:**
- Uses LevelDB for local metadata storage comparisons
- `CompareRaftMembers/followerDiff.js`: Compares follower databases against leader view
- `CompareRaftMembers/compareFollowerDbs.js`: Compares two follower database sets
- `CompareRaftMembers/repairObjects.js`: Automatically repairs detected inconsistencies
- Implements block digests for efficient comparison of large datasets
- Supports filtering by Raft oplog cseq to eliminate false positives from concurrent updates

**Stream Processing:**
- Extensive use of Node.js streams for memory-efficient processing of large datasets
- Custom transform streams for filtering, diffing, and aggregating metadata
- Examples: `DBListStream`, `DiffStream`, `BlockDigestsStream`, `RaftOplogStream`

### Technology Stack

- **Runtime:** Node.js 22+
- **AWS SDK:** v3 (client-s3, smithy packages)
- **Scality Libraries:**
  - `arsenal`: Core S3 data models and utilities
  - `bucketclient`: Bucket metadata client
  - `vaultclient`: Authentication service client
  - `werelogs`: Structured logging
- **Storage:** LevelDB for local metadata caching and comparison
- **Kafka:** node-rdkafka for replication queue management
- **Testing:** Jest with mongodb-memory-server
- **Monitoring:** Warp10 and Scuba for UtapiV2 metrics

### Environment Variable Patterns

Scripts follow consistent patterns for configuration:
- **Required:** `ACCESS_KEY`, `SECRET_KEY`, `ENDPOINT` for S3 operations
- **Optional:** `DEBUG=1` for debug logging, `WORKERS` for concurrency control
- **Limits:** `MAX_UPDATES`, `MAX_SCANNED` for batch processing with resumption
- **Markers:** `KEY_MARKER`, `VERSION_ID_MARKER` for resuming interrupted operations
- **Filters:** `TARGET_PREFIX`, `TARGET_REPLICATION_STATUS` for selective processing

### Testing Architecture

- **Unit Tests:** `tests/unit/` - Mock-based testing of individual components
- **Functional Tests:** `tests/functional/` - Integration tests requiring MongoDB and workbench
- **Workbench:** `workbench/env/default/` - Local development environment with CloudServer and dependencies
- **CI/CD:** GitHub Actions with Docker-based MongoDB and service orchestration
- Test configuration in `jest.config.js` with separate coverage directories

### Special Considerations

**Raft Metadata System:**
- Scripts interact with Raft-based distributed metadata storage
- Multiple raft sessions (typically 8) distribute metadata across nodes
- Leader-follower architecture requires special handling for consistency checks
- Oplog filtering prevents false positives from in-flight operations

**Sproxyd Storage:**
- Backend storage system for object data
- Scripts verify sproxyd key references in metadata match actual stored data
- Supports MPU (multipart upload) validation with multiple keys per object

**Replication Semantics:**
- Supports one-to-many replication to AWS S3, Azure Blob Storage, GCP
- Replication status lifecycle: NEW → PENDING → COMPLETED/FAILED
- REPLICA status indicates objects created via replication (not originals)

**Production Usage:**
- Scripts are designed for production operations on live systems
- Many scripts support dry-run modes for safety
- Progress logging and resumption markers for long-running operations
- Operations may require stopping Raft followers to ensure consistency
