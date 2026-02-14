# SwiftS3 Test Suite

This document describes the testing structure and best practices for the SwiftS3 project.

## Test Organization

The test suite is organized into several categories:

### Unit Tests
- **FileSystemStorageTests**: Tests for the file system storage backend
- **SQLStorageTests**: Tests for SQL metadata store operations
- **AccessControlPolicyTests**: Tests for ACL and policy evaluation

### Integration Tests
- **SwiftS3Tests**: End-to-end API testing with HTTP server
- **EndToEndIntegrationTests**: Comprehensive integration scenarios

### Performance Tests
- **PerformanceTests**: Benchmark tests with baseline performance assertions
- **StressTests**: High-load testing for stability

### Specialized Tests
- **ConcurrentTests**: Concurrency and thread safety testing
- **EdgeCaseTests**: Boundary condition and error path testing
- **ErrorPathTests**: Error simulation and recovery testing
- **EnterpriseFeaturesTests**: Advanced features like auditing, encryption, VPC

## Test Patterns

### Test Setup Pattern

All integration tests follow a consistent pattern using `withApp`:

```swift
func withApp(_ test: @escaping @Sendable (any TestClientProtocol) async throws -> Void) async throws {
    // Create isolated test environment
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
    let threadPool = NIOThreadPool(numberOfThreads: 2)
    threadPool.start()

    // Set up storage and server
    let storage = FileSystemStorage(rootPath: tempDir, metadataStore: store, testMode: true)

    // Create and test application
    let app = Application(router: router, configuration: config)
    try await app.test(.router, test)

    // Proper cleanup
    try await storage.shutdown()
    try await threadPool.shutdownGracefully()
    try await elg.shutdownGracefully()
}
```

### Test Mode

All storage backends support `testMode: true` which:
- Skips external network calls (SNS/SQS notifications)
- Prevents HTTP client creation to avoid shutdown issues
- Enables isolated testing without external dependencies

### Performance Baselines

Performance tests include baseline assertions:

```swift
#expect(avgPutTime < 0.1, "Average put time should be less than 100ms")
#expect(putTimes.filter { $0 > 1.0 }.count == 0, "No operation should take more than 1 second")
```

## Running Tests

### All Tests
```bash
swift test
```

### Specific Test Suite
```bash
swift test --filter PerformanceTests
```

### With Coverage
```bash
swift test --enable-code-coverage
./generate_coverage.sh
```

### CI/CD
Tests run automatically on GitHub Actions for macOS and Linux.

## Coverage Goals

Target: 90%+ code coverage

Current status: Coverage data generated in `.build/debug/codecov/`

## Best Practices

1. **Isolation**: Each test should be independent with its own resources
2. **Cleanup**: Always clean up resources in test teardown
3. **Test Mode**: Use `testMode: true` for storage backends
4. **Assertions**: Use `#expect` for test assertions
5. **Performance**: Include baseline checks in performance tests
6. **Concurrency**: Test concurrent operations for thread safety
7. **Error Paths**: Test both success and failure scenarios

## Adding New Tests

1. Choose appropriate test file based on functionality
2. Follow the `withApp` pattern for integration tests
3. Use `testMode: true` for storage initialization
4. Include proper error handling and cleanup
5. Add performance baselines for performance-critical code