# SwiftS3 Modernization Roadmap

This document outlines the roadmap to elevate SwiftS3 to modern standards, leveraging best practices from industry leaders like MinIO and AWS S3.

## 1. Core Architecture Modernization (Performance & Scalability)
**Goal:** Transition from a simple synchronous file server to a high-performance, non-blocking object engine.

- [x] **Non-blocking I/O**: Replace `FileManager` (blocking) with `SwiftNIO`'s `NIOFileSystem` for data path and metadata operations.
- [x] **Metadata Abstraction**: Refactor storage to use `MetadataStore` protocol.
- [x] **Metadata Engine**: Implement `SQLMetadataStore` using SQLite to replace sidecar files. This is critical for `ListObjects` performance.
- [x] **Streaming Data**: Fully streaming data paths for Uploads (Put) and Downloads (Get) using `AsyncStream` and `NIOFileSystem`.

## 2. Security & Identity (IAM)
**Goal:** Enterprise-grade security and access control.

- [ ] **Multi-User Identity**: Support multiple access keys/secrets via a configuration file or database.
- [ ] **Bucket Policies**: Implement JSON-based IAM policies for granular bucket/prefix access control.
- [ ] **ACLs**: Support basic Canned ACLs (private, public-read).
- [ ] **Signatures**: Verify `x-amz-content-sha256` payload checksums (currently only signature header is checked).

## 3. S3 Feature Parity (Compatibility)
**Goal:** Support the "Standard" S3 feature set expected by SDKs (boto3, AWS JS SDK).

- [ ] **Versioning**: Support object versioning (keeping multiple variants of an object).
- [ ] **Tagging**: Object and Bucket tagging support.
- [ ] **Lifecycle Rules**: Auto-deletion or transition of objects (TTL).
- [ ] **Presigned URLs**: Ensure compatibility with presigned URL generation.

## 4. Reliability & Operations
**Goal:** Production readiness.

- [ ] **Structured Logging**: JSON logs for observability.
- [ ] **Metrics**: Prometheus-compatible metrics endpoint (RPS, Latency, Storage usage).
- [ ] **checksum Verification**: Implement CRC32C/SHA256 checksums on upload/download.

## 5. Development Workflow Improvements
**Goal:** Ensure faster iteration cycles and better testing.

- [ ] **Unit Tests**: Coverage for edge cases (`FileSystemStorage` logic).
- [ ] **Integration Tests**: Docker-compose setup for real S3 client validation.
---
