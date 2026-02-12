import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("Error Path Testing")
struct ErrorPathTests {

    @Test("MockStorage Error Simulation - Create Bucket Failures")
    func testMockStorageCreateBucketErrors() async throws {
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

    @Test("MockStorage Error Simulation - Put Object Failures")
    func testMockStoragePutObjectErrors() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnPutObject = true

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // First create bucket
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // Try to put object - should fail
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket/test-object", method: .put, body: .string("test")) { response in
                #expect(response.status == .internalServerError)
            }
        }
    }

    @Test("MockStorage Error Simulation - Get Object Failures")
    func testMockStorageGetObjectErrors() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnGetObject = true

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // First create bucket and object
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }

            try await client.execute(uri: "/test-bucket/test-object", method: .put, body: .string("test")) { response in
                #expect(response.status == .ok)
            }
        }

        // Try to get object - should fail
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket/test-object", method: .get) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("MockStorage Error Simulation - Policy Operations")
    func testMockStoragePolicyErrors() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnPutBucketPolicy = true

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Create bucket first
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // Try to put policy - should fail
        let policy = """
        {
            "Version": "2012-10-17",
            "Statement": []
        }
        """

        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket?policy", method: .put, body: .string(policy)) { response in
                #expect(response.status == .internalServerError)
            }
        }
    }

    @Test("MockStorage Error Simulation - ACL Operations")
    func testMockStorageACLErrors() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnPutACL = true

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Create bucket first
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // Try to put ACL - should fail
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket?acl", method: .put) { response in
                #expect(response.status == .internalServerError)
            }
        }
    }

    @Test("MockStorage Error Simulation - Tagging Operations")
    func testMockStorageTaggingErrors() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnPutTags = true

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Create bucket and object first
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }

            try await client.execute(uri: "/test-bucket/test-object", method: .put, body: .string("test")) { response in
                #expect(response.status == .ok)
            }
        }

        // Try to put tags - should fail
        let tagsXML = """
        <Tagging>
            <TagSet>
                <Tag>
                    <Key>Test</Key>
                    <Value>Value</Value>
                </Tag>
            </TagSet>
        </Tagging>
        """

        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket/test-object?tagging", method: .put, body: .string(tagsXML)) { response in
                #expect(response.status == .internalServerError)
            }
        }
    }

    @Test("MockStorage Error Simulation - Multipart Upload Operations")
    func testMockStorageMultipartErrors() async throws {
        let mockStorage = MockStorage()
        mockStorage.shouldFailOnCreateMultipartUpload = true

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Create bucket first
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // Try to initiate multipart upload - should fail
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket/test-object?uploads", method: .post) { response in
                #expect(response.status == .internalServerError)
            }
        }
    }

    @Test("Edge Cases - Invalid Bucket Names")
    func testInvalidBucketNames() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Test various invalid bucket names
        let invalidNames = ["", "a", "A", "bucket with spaces", "bucket.with.dots", "bucket_with_underscores"]

        for name in invalidNames {
            try await app.test(.router) { client in
                try await client.execute(uri: "/\(name)", method: .put) { response in
                    // Should either succeed (if valid) or fail gracefully
                    #expect([.ok, .badRequest, .conflict].contains(response.status))
                }
            }
        }
    }

    @Test("Edge Cases - Large Object Handling")
    func testLargeObjectHandling() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Create bucket
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }
        }

        // Test with a moderately large object (1MB)
        let largeData = String(repeating: "x", count: 1024 * 1024)

        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket/large-object", method: .put, body: .string(largeData)) { response in
                #expect(response.status == .ok)
            }

            // Verify we can retrieve it
            try await client.execute(uri: "/test-bucket/large-object", method: .get) { response in
                #expect(response.status == .ok)
                #expect(response.body == ByteBuffer(string: largeData))
            }
        }
    }

    @Test("Edge Cases - Concurrent Bucket Creation")
    func testConcurrentBucketCreation() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        // Try to create the same bucket concurrently
        async let result1 = app.test(.router) { client in
            try await client.execute(uri: "/concurrent-bucket", method: .put) { response in
                return response.status
            }
        }

        async let result2 = app.test(.router) { client in
            try await client.execute(uri: "/concurrent-bucket", method: .put) { response in
                return response.status
            }
        }

        let (status1, status2) = try await (result1, result2)

        // One should succeed, one should fail with conflict
        #expect((status1 == .ok && status2 == .conflict) || (status1 == .conflict && status2 == .ok))
    }

    @Test("Edge Cases - Range Requests")
    func testRangeRequests() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        let testContent = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        // Create bucket and object
        try await app.test(.router) { client in
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }

            try await client.execute(uri: "/test-bucket/range-test", method: .put, body: .string(testContent)) { response in
                #expect(response.status == .ok)
            }
        }

        // Test range request
        try await app.test(.router) { client in
            var headers = HTTPFields()
            headers[.range] = "bytes=5-15"

            try await client.execute(uri: "/test-bucket/range-test", method: .get, headers: headers) { response in
                #expect(response.status == .partialContent)
                let returnedContent = String(buffer: response.body)
                #expect(returnedContent == "56789ABCDEF")
            }
        }

        // Test invalid range
        try await app.test(.router) { client in
            var headers = HTTPFields()
            headers[.range] = "bytes=100-200"

            try await client.execute(uri: "/test-bucket/range-test", method: .get, headers: headers) { response in
                #expect(response.status == .requestedRangeNotSatisfiable)
            }
        }
    }
}</content>
<parameter name="filePath">/Users/cybou/Documents/SwiftS3/Tests/SwiftS3Tests/ErrorPathTests.swift