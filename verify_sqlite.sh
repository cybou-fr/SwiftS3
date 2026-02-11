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

# 1. Create Bucket
echo "Creating bucket 'my-sqlite-bucket'..."
curl -v -X PUT http://localhost:8082/my-sqlite-bucket

# 2. Put Object
echo "Uploading object..."
echo "Hello SQLite World" > test.txt
curl -v -X PUT http://localhost:8082/my-sqlite-bucket/hello.txt --data-binary @test.txt

# 3. List Objects
echo "Listing objects..."
STORAGE_OUTPUT=$(curl -s http://localhost:8082/my-sqlite-bucket)
echo "Output: $STORAGE_OUTPUT"

if [[ "$STORAGE_OUTPUT" == *"hello.txt"* ]]; then
    echo "SUCCESS: Found hello.txt in listing"
else
    echo "FAILURE: Did not find hello.txt"
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
