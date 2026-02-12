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

- [x] **Multi-User Identity**: Support multiple users via SQLite database.
- [x] **Bucket Policies**: Implement JSON-based IAM policies for granular bucket/prefix access control.
- [x] **ACLs**: Support basic Canned ACLs (private, public-read).
- [x] **Signatures**: Verify `x-amz-content-sha256` payload checksums (currently only signature header is checked).

## 3. S3 Feature Parity (Compatibility)
**Goal:** Support the "Standard" S3 feature set expected by SDKs (boto3, AWS JS SDK).

- [x] **Versioning**: Support object versioning (keeping multiple variants of an object).
- [x] **Tagging**: Object and Bucket tagging support.
- [/] **Lifecycle Rules**: Expiration (Days) implemented. Next: Noncurrent version expiration, pagination for large buckets, and Prefix/Tag filtering improvements.
- [x] **Presigned URLs**: Full support for query-parameter based authentication.
- [ ] **MFA Delete**: Support Multi-Factor Authentication for sensitive operations.

## 4. Reliability & Operations
**Goal:** Production readiness.

- [x] **Structured Logging**: JSON logs for observability.
- [ ] **Metrics**: Prometheus-compatible metrics endpoint (RPS, Latency, Storage usage).
- [x] **Checksum Verification**: Implement CRC32C/SHA256 checksums on upload/download.
- [ ] **Garbage Collection**: Cleanup of orphaned files or failed multipart uploads (Janitor expansion).

## 5. Development Workflow Improvements
**Goal:** Ensure faster iteration cycles and better testing.

- [x] **Unit Tests**: Coverage for edge cases (`FileSystemStorage` logic).
- [x] **Integration Tests**: Tests for real S3 client validation using `HummingbirdTesting`.
- [/] **E2E Tests**: Validate with real AWS CLI and S3 SDKs (ongoing).
---
