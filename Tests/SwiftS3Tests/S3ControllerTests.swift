import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import NIOHTTP1
import Testing

@testable import SwiftS3

@Suite("S3Controller Unit Tests")
/// Unit tests for S3Controller focusing on request handling and business logic.
/// Uses MockStorage for isolated testing of controller behavior without file system dependencies.
/// Tests cover bucket operations, error handling, and authentication integration.
struct S3ControllerTests {

    /// Creates a test application instance with S3Controller and mock storage.
    /// Sets up routing, error handling middleware, and mock authentication for unit testing.
    /// Provides isolated test environment without external dependencies.
    ///
    /// - Parameter storage: Mock storage backend to use for the test
    /// - Returns: Configured Application instance ready for testing
    private func createTestApp(storage: MockStorage) -> some ApplicationProtocol {
        let controller = S3Controller(storage: storage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        return Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))
    }

    @Test("Create bucket succeeds")
    func testCreateBucket() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Create bucket fails when storage fails")
    func testCreateBucketFailure() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnCreateBucket = true
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .conflict) // BucketAlreadyExists
            }
        }
    }

    @Test("Delete bucket succeeds")
    func testDeleteBucket() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // First create the bucket
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // Then delete it
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .delete) { response in
                #expect(response.status == .noContent)
            }
        }
    }

    @Test("Delete non-existent bucket fails")
    func testDeleteNonExistentBucket() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        try await app.test(.router) { client in
            try await client.execute(uri: "/non-existent-bucket", method: .delete) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("List buckets returns empty list initially")
    func testListBucketsEmpty() async throws {
        let mockStorage = MockStorage()
        let app = createTestApp(storage: mockStorage)

        try await app.test(.router) { client in
            try await client.execute(uri: "/", method: .get) { response in
                #expect(response.status == .ok)
                let body = try #require(String(buffer: response.body))
                #expect(body.contains("<ListAllMyBucketsResult"))
                #expect(body.contains("<Buckets>"))
                #expect(!body.contains("<Bucket>"))
            }
        }
    }

    @Test("List buckets returns created buckets")
    func testListBucketsWithBuckets() async throws {
        let mockStorage = MockStorage()
        let app = createTestApp(storage: mockStorage)

        // Create a bucket first
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // List buckets
        try await app.test(.router) { client in
            try await client.execute(uri: "/", method: .get) { response in
                #expect(response.status == .ok)
                let body = try #require(String(buffer: response.body))
                #expect(body.contains("<ListAllMyBucketsResult"))
                #expect(body.contains("test-bucket"))
            }
        }
    }
}

// Mock authenticator for testing
/// Test middleware that bypasses real authentication for unit tests.
/// Automatically sets the principal to "test-user" for all requests.
/// Allows controller logic testing without complex signature verification.
struct MockAuthenticatorMiddleware: RouterMiddleware {
    typealias Context = S3RequestContext

    func handle(_ request: Input, context: Context, next: (Input, Context) async throws -> Output)
        async throws -> Output
    {
        var context = context
        context.principal = "test-user"
        return try await next(request, context)
    }
}
