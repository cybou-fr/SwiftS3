# SwiftS3

SwiftS3 is a lightweight, S3-compatible object storage server written in Swift. Built on top of [Hummingbird](https://github.com/hummingbird-project/hummingbird), it provides a simple and efficient way to run a local S3-like service for development and testing purposes.

## Features

- **S3 Compatibility**: Supports core S3 API operations giving you a familiar interface.
- **SQLite Metadata Engine**: Uses a high-performance SQLite engine for managing object metadata, enabling fast listing, searching, and filtering of billions of objects.
- **Non-blocking Architecture**: Built with [SwiftNIO](https://github.com/apple/swift-nio) and `_NIOFileSystem` for fully asynchronous, non-blocking I/O, ensuring high throughput and responsiveness.
- **Streaming Data Paths**: Implements streaming for both uploads and downloads, supporting massive objects with minimal memory overhead.
- **AWS Signature V4 Auth**: Implements industry-standard AWS Signature Version 4 authentication for secure and compatible requests.
- **Bucket Policies**: Supports JSON-based IAM policies for granular access control.
- **Extensible Storage**: Modular architecture allowing for different storage backends and metadata stores.

## Supported Operations

### Buckets
- **List Buckets**: `GET /`
- **Create Bucket**: `PUT /:bucket`
- **Delete Bucket**: `DELETE /:bucket`
- **List Objects**: `GET /:bucket`
    - Supports `prefix`, `delimiter`, `marker`, and `max-keys` query parameters.
- **Bucket Policy**:
    - **Put Policy**: `PUT /:bucket?policy`
    - **Get Policy**: `GET /:bucket?policy`
    - **Delete Policy**: `DELETE /:bucket?policy`

### Objects
- **Put Object**: `PUT /:bucket/:key`
    - Supports `x-amz-meta-*` headers for custom metadata.
    - Supports `Content-Type` persistence.
    - Supports `x-amz-copy-source` for copying objects.
- **Get Object**: `GET /:bucket/:key`
    - Supports `Range` header for partial content (e.g., `bytes=0-10`).
    - Returns persisted metadata and content type.
- **Delete Object**: `DELETE /:bucket/:key`
- **Head Object**: `HEAD /:bucket/:key`

### Multipart Upload
- **Initiate Multipart Upload**: `POST /:bucket/:key?uploads`
- **Upload Part**: `PUT /:bucket/:key?partNumber=:partNumber&uploadId=:uploadId`
- **Complete Multipart Upload**: `POST /:bucket/:key?uploadId=:uploadId`
- **Abort Multipart Upload**: `DELETE /:bucket/:key?uploadId=:uploadId`

## Requirements

- Swift 6.0 or later
- macOS 14.0 or later (or Linux equivalent)

## Installation

SwiftS3 is a Swift executable package. You can build and run it using the Swift Package Manager.

1. Clone the repository:
   ```bash
   git clone https://github.com/cybou-fr/SwiftS3.git
   cd SwiftS3
   ```

2. Build the project:
   ```bash
   swift build -c release
   ```

## Usage

To start the server, run the executable. By default, it binds to `127.0.0.1:8080`.

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

## Authentication

SwiftS3 enforces AWS Signature V4 authentication.

**Default Credentials (MVP):**
- **Access Key ID**: `admin`
- **Secret Access Key**: `password`
- **Region**: `us-east-1` (or any valid region string)

Users are stored in the SQLite metadata database (`users` table). You can manage users programmatically via the `UserStore` interface (API endpoints for user management are planned).

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

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.
