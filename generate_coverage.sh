#!/bin/bash

# Generate code coverage report for SwiftS3

echo "Running tests with code coverage..."
swift test --enable-code-coverage

if [ $? -eq 0 ]; then
    echo "Tests passed. Generating coverage report..."

    # Find the coverage file
    COVERAGE_DIR=".build/debug/codecov"
    if [ -d "$COVERAGE_DIR" ]; then
        echo "Coverage data found in $COVERAGE_DIR"
        # You can use tools like lcov or xcov to generate reports
        # For now, just list the files
        ls -la "$COVERAGE_DIR"
    else
        echo "Coverage directory not found. Check if coverage was enabled correctly."
    fi
else
    echo "Tests failed. Cannot generate coverage report."
    exit 1
fi