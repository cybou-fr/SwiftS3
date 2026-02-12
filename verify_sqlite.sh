#!/bin/bash
set -e

# Cleanup previous run
rm -rf ./data-verify
mkdir -p ./data-verify

# Compile first to avoid timeout in background run
swift build

# Start server
echo "Starting S3 Server..."
# Using --storage ./data-verify and --port 8082
swift run SwiftS3 --storage ./data-verify --port 8082 > server.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server (15s)..."
sleep 15
cat server.log

# Function to cleanup on exit
cleanup() {
    echo "Stopping server..."
    kill $SERVER_PID
    wait $SERVER_PID 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

# 1. Configure AWS CLI
echo "Configuring AWS CLI..."
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:8082

# 2. Create Bucket
echo "Creating bucket 'my-sqlite-bucket'..."
aws s3 mb s3://my-sqlite-bucket

# 3. Put Object
echo "Uploading object..."
echo "Hello SQLite World" > test.txt
aws s3 cp test.txt s3://my-sqlite-bucket/hello.txt

# 4. List Objects
echo "Listing objects..."
aws s3 ls s3://my-sqlite-bucket

# 5. Get Object
echo "Downloading object..."
aws s3 cp s3://my-sqlite-bucket/hello.txt downloaded.txt
if cmp -s test.txt downloaded.txt; then
    echo "SUCCESS: Downloaded file matches uploaded file"
else
    echo "FAILURE: Downloaded file does not match"
    exit 1
fi

# 4. Check SQLite file existence
if [ -f "./data-verify/metadata.sqlite" ]; then
    echo "SUCCESS: metadata.sqlite exists"
    ls -l ./data-verify/metadata.sqlite
else
    echo "FAILURE: metadata.sqlite missing"
    exit 1
fi
