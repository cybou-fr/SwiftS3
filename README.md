# SwiftS3

[![Swift](https://img.shields.io/badge/Swift-6.0+-orange.svg)](https://swift.org)
[![macOS](https://img.shields.io/badge/macOS-14.0+-blue.svg)](https://www.apple.com/macos/)
[![Linux](https://img.shields.io/badge/Linux-Compatible-green.svg)](https://www.linux.org/)

SwiftS3 is a lightweight, S3-compatible object storage server written in Swift. Built on top of [Hummingbird](https://github.com/hummingbird-project/hummingbird), it provides a simple and efficient way to run a local S3-like service for development and testing purposes.

## Features

- **S3 Compatibility**: Supports core S3 API operations giving you a familiar interface.
- **SQLite Metadata Engine**: Uses SQLite for managing object metadata, enabling fast listing, searching, and filtering.
- **Non-blocking Architecture**: Built with [SwiftNIO](https://github.com/apple/swift-nio) and `_NIOFileSystem` for asynchronous, non-blocking I/O.
- **Streaming Data Paths**: Supports streaming for both uploads and downloads, handling large objects efficiently.
- **AWS Signature V4 Auth**: Implements AWS Signature Version 4 authentication for secure and compatible requests.
- **Bucket Policies**: Supports JSON-based IAM policies for granular access control.
- **Access Control Lists (ACLs)**: Supports canned ACLs (private, public-read, etc.) for buckets and objects.
- **Object Versioning**: Supports keeping multiple versions of the same object, including delete markers and restoration of previous versions.
- **Object Tagging**: Support for categorizing objects with key-value pairs.
- **Checksum Verification**: Automatic SHA256/CRC32C checksum verification on upload and download.
- **Lifecycle Management**: Periodical expiration of old objects based on bucket rules.
- **Extensible Storage**: Modular architecture allowing for different storage backends and metadata stores.

### Enterprise Features

- **Server-Side Encryption (SSE-KMS)**: AES256 and AWS KMS-compatible encryption for data at rest.
- **VPC-Only Access**: Restrict bucket access to specific IP ranges for enhanced security.
- **Advanced Auditing**: Comprehensive audit logging with compliance reporting and security monitoring.
- **Analytics & Insights**: Storage analytics, access analyzer, inventory reports, and performance metrics.
- **Batch Operations**: Large-scale batch operations on objects (like AWS S3 Batch).
- **Event Notifications**: S3-compatible event notifications for object operations, including webhook support and message queue integration.
- **Identity Federation**: LDAP/Active Directory integration for enterprise authentication.
- **Cross-Region Replication**: Automatic replication of objects across multiple regions.
- **Object Lock**: WORM (Write Once Read Many) compliance with retention periods and legal holds.
- **S3 Select**: SQL queries directly on objects without downloading.
- **Multipart Upload Copy**: Copy parts from existing objects during multipart uploads.

## Architecture

SwiftS3 follows a modular architecture:

- **Controllers**: Handle HTTP requests and responses for S3 API operations
- **Storage Backend**: Abstract interface for object storage (currently file system based)
- **Metadata Store**: Abstract interface for metadata persistence (currently SQLite based)
- **Authentication**: AWS Signature V4 verification and user management
- **Lifecycle Management**: Background process for object expiration and cleanup

## Supported Operations

### Buckets
- **List Buckets**: `GET /`
- **Create Bucket**: `PUT /:bucket`
- **Delete Bucket**: `DELETE /:bucket`
- **List Objects**: `GET /:bucket`
    - Supports V1 and V2 (`list-type=2`).
    - Supports `prefix`, `delimiter`, `marker`, and `max-keys` query parameters.
- **List Object Versions**: `GET /:bucket?versions`
- **Bucket ACL**:
    - **Get ACL**: `GET /:bucket?acl`
    - **Put ACL**: `PUT /:bucket?acl`
- **Bucket Versioning**:
    - **Get Versioning**: `GET /:bucket?versioning`
    - **Put Versioning**: `PUT /:bucket?versioning`
- **Bucket Policy**:
    - **Put Policy**: `PUT /:bucket?policy`
    - **Get Policy**: `GET /:bucket?policy`
    - **Delete Policy**: `DELETE /:bucket?policy`
- **Bucket Encryption**:
    - **Put Encryption**: `PUT /:bucket?encryption`
    - **Get Encryption**: `GET /:bucket?encryption`
- **Bucket Replication**:
    - **Put Replication**: `PUT /:bucket?replication`
    - **Get Replication**: `GET /:bucket?replication`
    - **Delete Replication**: `DELETE /:bucket?replication`
- **Bucket Notifications**:
    - **Put Notification**: `PUT /:bucket?notification`
    - **Get Notification**: `GET /:bucket?notification`
- **Bucket VPC Configuration**:
    - **Put VPC Config**: `PUT /:bucket?vpc`
    - **Delete VPC Config**: `DELETE /:bucket?vpc`
- **Bucket Object Lock**:
    - **Put Object Lock**: `PUT /:bucket?object-lock`
    - **Get Object Lock**: `GET /:bucket?object-lock`

### Audit & Compliance
- **Get Audit Events**: `GET /audit` or `GET /:bucket/audit`
    - Supports filtering by principal, event type, date range, and pagination.
- **Delete Audit Events**: `DELETE /audit`
    - Bulk cleanup of audit logs older than specified date.

## Enterprise Features

### Server-Side Encryption (SSE)

SwiftS3 supports server-side encryption with AES256 and AWS KMS-compatible encryption:

```bash
# Configure bucket encryption
aws s3api put-bucket-encryption \
  --bucket mybucket \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }
    ]
  }' \
  --endpoint-url http://localhost:8080
```

### VPC-Only Access

Restrict bucket access to specific IP ranges for enhanced security:

```bash
# Configure VPC access for a bucket
curl -X PUT "http://localhost:8080/mybucket?vpc" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "Content-Type: application/json" \
  -d '{
    "VpcId": "vpc-12345",
    "AllowedIpRanges": ["10.0.0.0/8", "192.168.1.0/24"]
  }'
```

### Advanced Auditing

Comprehensive audit logging for compliance and security monitoring:

```bash
# Get audit events for a bucket
curl "http://localhost:8080/mybucket/audit?maxItems=50" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Get global audit events
curl "http://localhost:8080/audit?eventType=AccessDenied&startDate=2024-01-01T00:00:00Z" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Event Notifications

Configure S3-compatible event notifications:

```bash
# Configure bucket notifications
aws s3api put-bucket-notification-configuration \
  --bucket mybucket \
  --notification-configuration '{
    "TopicConfigurations": [
      {
        "Id": "notification-1",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [
              {
                "Name": "prefix",
                "Value": "logs/"
              }
            ]
          }
        }
      }
    ]
  }' \
  --endpoint-url http://localhost:8080
```

### Cross-Region Replication

Configure automatic replication across regions:

```bash
# Configure replication
aws s3api put-bucket-replication \
  --bucket source-bucket \
  --replication-configuration '{
    "Role": "arn:aws:iam::123456789012:role/replication-role",
    "Rules": [
      {
        "ID": "rule1",
        "Status": "Enabled",
        "Prefix": "documents/",
        "Destination": {
          "Bucket": "arn:aws:s3:::destination-bucket"
        }
      }
    ]
  }' \
  --endpoint-url http://localhost:8080
```

### Object Lock

Implement WORM (Write Once Read Many) compliance:

```bash
# Enable object lock on bucket
aws s3api put-object-lock-configuration \
  --bucket mybucket \
  --object-lock-configuration '{
    "ObjectLockEnabled": "Enabled",
    "Rule": {
      "DefaultRetention": {
        "Mode": "COMPLIANCE",
        "Days": 365
      }
    }
  }' \
  --endpoint-url http://localhost:8080

# Put object lock on specific object
aws s3api put-object-lock \
  --bucket mybucket \
  --key important-document.pdf \
  --lock-mode COMPLIANCE \
  --lock-retain-until-date 2025-01-01T00:00:00Z \
  --endpoint-url http://localhost:8080
```

### Analytics & Insights

SwiftS3 provides comprehensive analytics and insights for storage optimization and security monitoring.

#### Storage Analytics

Get usage analytics, access patterns, and cost optimization insights:

```bash
# Get storage analytics for all buckets (last 30 days)
curl "http://localhost:8080/analytics/storage" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Get analytics for specific bucket (last 7 days)
curl "http://localhost:8080/analytics/storage?bucket=mybucket&period=7" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

#### Access Analyzer

Analyze bucket access patterns for security issues:

```bash
# Analyze access patterns for a bucket (last 7 days)
curl "http://localhost:8080/mybucket/analytics/access-analyzer" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Analyze with custom period
curl "http://localhost:8080/mybucket/analytics/access-analyzer?period=30" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

#### Inventory Reports

Generate automated inventory reports with metadata:

```bash
# Get JSON inventory for all objects in bucket
curl "http://localhost:8080/mybucket/inventory" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Get CSV inventory with prefix filter
curl "http://localhost:8080/mybucket/inventory?format=csv&prefix=logs/" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

#### Performance Metrics

Get detailed performance monitoring and optimization insights:

```bash
# Get performance metrics (last 24 hours)
curl "http://localhost:8080/analytics/performance" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Get metrics for custom period
curl "http://localhost:8080/analytics/performance?period=168" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

#### Prometheus Metrics

Basic Prometheus-compatible metrics for monitoring:

```bash
# Get Prometheus metrics
curl "http://localhost:8080/metrics"
```

### Batch Operations

SwiftS3 supports large-scale batch operations on objects, similar to AWS S3 Batch Operations. You can perform operations like copying, tagging, and deleting objects in bulk using CSV manifests.

#### Create a Batch Job

Create a batch job to copy objects to a different bucket:

```bash
# First, create a manifest CSV file in your bucket
echo "Bucket,Key" > manifest.csv
echo "my-bucket,object1.txt" >> manifest.csv
echo "my-bucket,object2.txt" >> manifest.csv

# Upload the manifest
aws s3 cp manifest.csv s3://my-bucket/manifest.csv --endpoint-url http://localhost:8080

# Create the batch job
curl -X POST "http://localhost:8080/batch/job" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "Content-Type: application/json" \
  -d '{
    "operation": {
      "type": "S3PutObjectCopy",
      "parameters": {
        "targetBucket": "destination-bucket",
        "targetPrefix": "copied/"
      }
    },
    "manifest": {
      "location": {
        "bucket": "my-bucket",
        "key": "manifest.csv"
      },
      "spec": {
        "format": "S3BatchOperations_CSV_20180820",
        "fields": ["Bucket", "Key"]
      }
    },
    "priority": 1
  }'
```

#### Monitor Batch Jobs

Check the status of your batch jobs:

```bash
# List all batch jobs
curl "http://localhost:8080/batch/jobs" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Get specific job details
curl "http://localhost:8080/batch/job/{job-id}" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

#### Supported Operations

- **S3PutObjectCopy**: Copy objects to a different location
- **S3PutObjectAcl**: Modify object ACLs
- **S3PutObjectTagging**: Add or modify object tags
- **S3DeleteObject**: Delete objects
- **S3InitiateRestoreObject**: Restore objects from archive
- **S3PutObjectLegalHold**: Set legal hold on objects
- **S3PutObjectRetention**: Set retention period on objects

## API Endpoints

### Buckets
- **List Buckets**: `GET /`
- **Create Bucket**: `PUT /:bucket`
- **Delete Bucket**: `DELETE /:bucket`
- **List Objects**: `GET /:bucket`
    - Supports V1 and V2 (`list-type=2`).
    - Supports `prefix`, `delimiter`, `marker`, and `max-keys` query parameters.
- **List Object Versions**: `GET /:bucket?versions`
- **Bucket ACL**:
    - **Get ACL**: `GET /:bucket?acl`
    - **Put ACL**: `PUT /:bucket?acl`
- **Bucket Versioning**:
    - **Get Versioning**: `GET /:bucket?versioning`
    - **Put Versioning**: `PUT /:bucket?versioning`
- **Bucket Policy**:
    - **Put Policy**: `PUT /:bucket?policy`
    - **Get Policy**: `GET /:bucket?policy`
    - **Delete Policy**: `DELETE /:bucket?policy`
- **Bucket Encryption**:
    - **Put Encryption**: `PUT /:bucket?encryption`
    - **Get Encryption**: `GET /:bucket?encryption`
- **Bucket Replication**:
    - **Put Replication**: `PUT /:bucket?replication`
    - **Get Replication**: `GET /:bucket?replication`
    - **Delete Replication**: `DELETE /:bucket?replication`
- **Bucket Notifications**:
    - **Put Notification**: `PUT /:bucket?notification`
    - **Get Notification**: `GET /:bucket?notification`
- **Bucket VPC Configuration**:
    - **Put VPC Config**: `PUT /:bucket?vpc`
    - **Delete VPC Config**: `DELETE /:bucket?vpc`
- **Bucket Object Lock**:
    - **Put Object Lock**: `PUT /:bucket?object-lock`
    - **Get Object Lock**: `GET /:bucket?object-lock`

### Audit & Compliance
- **Get Audit Events**: `GET /audit` or `GET /:bucket/audit`
    - Supports filtering by principal, event type, date range, and pagination.
- **Delete Audit Events**: `DELETE /audit`
    - Bulk cleanup of audit logs older than specified date.

### Analytics & Insights
- **Storage Analytics**: `GET /analytics/storage`
    - Usage analytics, access patterns, and cost optimization insights.
    - Supports `period` (days) and `bucket` filter query parameters.
- **Access Analyzer**: `GET /:bucket/analytics/access-analyzer`
    - Security analysis for bucket access patterns.
    - Supports `period` (days) query parameter.
- **Bucket Inventory**: `GET /:bucket/inventory`
    - Automated inventory generation with metadata.
    - Supports `format` (json/csv) and `prefix` filter query parameters.
- **Performance Metrics**: `GET /analytics/performance`
    - Detailed performance monitoring and optimization.
    - Supports `period` (hours) query parameter.
- **Basic Metrics**: `GET /metrics`
    - Prometheus-compatible metrics for monitoring.

### Batch Operations
- **Create Batch Job**: `POST /batch/job`
    - Create a new batch job for large-scale operations.
- **Get Batch Job**: `GET /batch/job/:jobId`
    - Retrieve information about a specific batch job.
- **List Batch Jobs**: `GET /batch/jobs`
    - List batch jobs with optional filtering by bucket and status.
- **Update Batch Job Status**: `PUT /batch/job/:jobId/status`
    - Update the status of a batch job (pause, resume, cancel).
- **Delete Batch Job**: `DELETE /batch/job/:jobId`
    - Delete a completed or failed batch job.

- **Put Object**: `PUT /:bucket/:key`
    - Supports `x-amz-meta-*` headers for custom metadata.
    - Supports `Content-Type` persistence.
    - Supports `x-amz-copy-source` for copying objects.
    - Automatically creates new versions if versioning is enabled.
- **Get Object**: `GET /:bucket/:key`
    - Supports `versionId` query parameter.
    - Supports `Range` header for partial content.
- **Delete Object**: `DELETE /:bucket/:key`
    - Supports `versionId` query parameter for permanent deletion.
    - Creates a **Delete Marker** if no `versionId` is specified and versioning is enabled.
- **Head Object**: `HEAD /:bucket/:key`
    - Supports `versionId` query parameter.
- **Object ACL**:
    - **Get ACL**: `GET /:bucket/:key?acl`
    - **Put ACL**: `PUT /:bucket/:key?acl` (supports `versionId`)

### Multipart Upload
- **Initiate Multipart Upload**: `POST /:bucket/:key?uploads`
- **Upload Part**: `PUT /:bucket/:key?partNumber=:partNumber&uploadId=:uploadId`
- **Complete Multipart Upload**: `POST /:bucket/:key?uploadId=:uploadId`
- **Abort Multipart Upload**: `DELETE /:bucket/:key?uploadId=:uploadId`

## Requirements

- Swift 6.0 or later
- macOS 14.0 or later (Tested on macOS 14.x and 15.x)
- Linux (Requires Swift 6.0+ toolchain)

## Installation

SwiftS3 is a Swift executable package. You can build and run it using the Swift Package Manager.

### Clone and Build

1. Clone the repository:
   ```bash
   git clone https://github.com/cybou-fr/SwiftS3.git
   cd SwiftS3
   ```

2. Build the project:
   ```bash
   swift build -c release
   ```

3. Run the server:
   ```bash
   swift run SwiftS3
   ```

### Using Swift Package Manager

You can also add SwiftS3 as a dependency to your own Swift project:

```swift
dependencies: [
    .package(url: "https://github.com/cybou-fr/SwiftS3.git", from: "1.0.0")
]
```

## Usage

### Basic Usage

To start the server with default settings (binds to `127.0.0.1:8080`):

```bash
swift run SwiftS3
```

### Configuration Options

You can configure the server using command-line arguments:

| Option       | Shorthand | Default     | Description                      |
|--------------|-----------|-------------|----------------------------------|
| `--port`     |    `-p`   |    `8080`   | Port to bind the server to.      |
| `--hostname` |    `-h`   | `127.0.0.1` | Hostname to bind the server to.  |
| `--storage`  |    `-s`   | `./data`    | Directory path for storing data. |

**Example:**

Run the server on port `3000` with a custom storage directory:

```bash
swift run SwiftS3 --port 3000 --storage /path/to/my/storage
```

### User Management

SwiftS3 includes a command-line interface for managing users:

```bash
# Create a new user
swift run SwiftS3 user create myuser --access-key MYACCESSKEY --secret-key MYSECRETKEY

# List all users
swift run SwiftS3 user list

# Delete a user
swift run SwiftS3 user delete MYACCESSKEY
```

## Authentication

SwiftS3 enforces AWS Signature V4 authentication.

**Default Credentials (for development):**
- **Access Key ID**: `admin`
- **Secret Access Key**: `password`
- **Region**: `us-east-1` (or any valid region string)

Users are stored in the SQLite metadata database (`users` table). You can manage users programmatically via the `UserStore` interface.

### Example with AWS CLI

Configure a profile or use environment variables:

```bash
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_DEFAULT_REGION=us-east-1
```

List buckets:

```bash
aws s3 ls --endpoint-url http://localhost:8080
```

Upload a file:

```bash
aws s3 cp myfile.txt s3://mybucket/myfile.txt --endpoint-url http://localhost:8080
```

Download a file:

```bash
aws s3 cp s3://mybucket/myfile.txt downloaded.txt --endpoint-url http://localhost:8080
```

Create a bucket:

```bash
aws s3 mb s3://mybucket --endpoint-url http://localhost:8080
```

### Example with MinIO Client (mc)

```bash
# Configure host
mc alias set swiftS3 http://localhost:8080 admin password

# List buckets
mc ls swiftS3

# Upload file
mc cp myfile.txt swiftS3/mybucket/
```

## Testing

SwiftS3 includes a comprehensive test suite. To run the tests:

```bash
swift test
```

The test suite covers:
- Unit tests for individual components
- Integration tests for end-to-end S3 API compatibility
- Performance benchmarks
- Concurrent operation testing
- Error path testing
- Enterprise feature testing (SSE, VPC, Auditing, Event Notifications, Replication)

### Test Coverage

- **Core S3 Operations**: Bucket and object CRUD operations
- **Authentication & Authorization**: AWS Signature V4, policies, ACLs
- **Versioning & Lifecycle**: Object versioning, expiration rules
- **Enterprise Features**: Server-side encryption, VPC access control, audit logging, event notifications, replication, object lock
- **Storage Backends**: File system and SQLite metadata store implementations
- **Error Handling**: Comprehensive error path testing
- **Performance**: Concurrent operations and benchmark testing

## Development

### Project Structure

```
Sources/
├── SwiftS3/
│   ├── Controllers/          # HTTP request handlers
│   ├── Storage/             # Storage backend implementations
│   ├── Auth/                # Authentication and authorization
│   ├── XML.swift            # XML response generation
│   ├── XMLBuilder.swift     # XML construction utilities
│   └── *.swift              # Core server components
Tests/
└── SwiftS3Tests/            # Test suite
```

### Key Components

- **S3Controller**: Main request router and handler
- **StorageBackend**: Protocol for storage implementations
- **MetadataStore**: Protocol for metadata persistence
- **S3Authenticator**: AWS Signature V4 verification
- **LifecycleJanitor**: Background cleanup process

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass: `swift test`
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Hummingbird](https://github.com/hummingbird-project/hummingbird) - The web framework
- [SwiftNIO](https://github.com/apple/swift-nio) - Networking library
- [SQLiteNIO](https://github.com/vapor/sqlite-nio) - SQLite database driver
- AWS S3 API documentation for compatibility reference
