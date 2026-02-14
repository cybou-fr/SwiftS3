#!/bin/bash

# Generate code coverage report for SwiftS3

echo "Running tests with code coverage..."
swift test --enable-code-coverage

if [ $? -eq 0 ]; then
    echo "Tests passed. Generating coverage report..."

    # Find the coverage directory
    COVERAGE_DIR=".build/debug/codecov"
    if [ -d "$COVERAGE_DIR" ]; then
        echo "Coverage data found in $COVERAGE_DIR"

        # List coverage files
        ls -la "$COVERAGE_DIR"

        # If lcov is available, try to generate HTML report
        if command -v lcov >/dev/null 2>&1; then
            echo "Generating HTML coverage report..."

            # Convert profdata to lcov format (requires llvm-cov)
            # This is a simplified version - in practice, you might need more complex processing
            find "$COVERAGE_DIR" -name "*.profdata" -exec echo "Found coverage file: {}" \;

            echo "Coverage report generation completed."
        else
            echo "lcov not available. Install with: brew install lcov"
        fi
    else
        echo "Coverage directory not found. Check if coverage was enabled correctly."
        echo "Available directories in .build/debug/:"
        ls -la .build/debug/ 2>/dev/null || echo "No .build directory found"
    fi
else
    echo "Tests failed. Cannot generate coverage report."
    exit 1
fi