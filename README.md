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

### Objects
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
