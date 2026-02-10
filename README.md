# SwiftS3

SwiftS3 is a lightweight, S3-compatible object storage server written in Swift. Built on top of [Hummingbird](https://github.com/hummingbird-project/hummingbird), it provides a simple and efficient way to run a local S3-like service for development and testing purposes.

## Features

- **S3 Compatibility**: Supports core S3 API operations giving you a familiar interface.
- **File System Storage**: Stores objects and buckets directly on the local file system.
- **AWS Signature V4 Auth**: Implements AWS Signature Version 4 authentication for secure requests.
- **High Performance**: Built with Swift and Hummingbird for valid asynchronous input/output.

## Supported Operations

### Buckets
- **List Buckets**: `GET /`
- **Create Bucket**: `PUT /:bucket`
- **Delete Bucket**: `DELETE /:bucket`
- **List Objects**: `GET /:bucket`

### Objects
- **Put Object**: `PUT /:bucket/:key`
- **Get Object**: `GET /:bucket/:key`
- **Delete Object**: `DELETE /:bucket/:key`
- **Head Object**: `HEAD /:bucket/:key`

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

| Option | Shorthand | Default | Description |
|--------|-----------|---------|-------------|
| `--port` | `-p` | `8080` | Port to bind the server to. |
| `--hostname` | `-h` | `127.0.0.1` | Hostname to bind the server to. |
| `--storage` | `-s` | `./data` | Directory path for storing data. |

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

You can use these credentials with tools like the AWS CLI or SDKs.

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
