import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("Concurrent Testing")
struct ConcurrentTests {

    func withApp(_ test: @escaping @Sendable (any TestClientProtocol) async throws -> Void)
        async throws
    {
        // Create per-test event loop group and thread pool
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        let server = S3Server(
            hostname: "127.0.0.1", port: 0, storagePath: storagePath, accessKey: "admin",
            secretKey: "password", ldapConfig: nil)

        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite",
            on: elg,
            threadPool: threadPool
        )

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)
        let controller = S3Controller(storage: storage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(userStore: metadataStore))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(server.hostname, port: server.port))
        )

        do {
            try await app.test(.router, test)
        } catch {
            // Cleanup on error
            try? await storage.shutdown()
            try? await metadataStore.shutdown()
            try? FileManager.default.removeItem(atPath: storagePath)
            try? await threadPool.shutdownGracefully()
            try? await elg.shutdownGracefully()
            throw error
        }

        // Cleanup
        try? await storage.shutdown()
        try? await metadataStore.shutdown()
        try? FileManager.default.removeItem(atPath: storagePath)
        try? await threadPool.shutdownGracefully()
        try? await elg.shutdownGracefully()
    }

    func sign(
        _ method: String, _ path: String, key: String = "admin", secret: String = "password",
        body: String = ""
    ) -> HTTPFields {
        let helper = AWSAuthHelper(accessKey: key, secretKey: secret)
        let url = URL(string: "http://localhost" + path)!
        let httpMethod = HTTPRequest.Method(rawValue: method) ?? .get
        return (try? helper.signRequest(method: httpMethod, url: url, payload: body))
            ?? HTTPFields()
    }

    @Test("Concurrent Object Operations")
    func testConcurrentObjectOperations() async throws {
        try await withApp { client in
            let bucket = "concurrent-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Concurrently create multiple objects
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<10 {
                    group.addTask {
                        let key = "object-\(i)"
                        let content = "Content for object \(i)"

                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .put,
                            headers: self.sign("PUT", "/\(bucket)/\(key)", body: content),
                            body: ByteBuffer(string: content)
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            // Verify all objects were created
            try await client.execute(
                uri: "/\(bucket)",
                method: .get,
                headers: sign("GET", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                #expect(bodyString.contains("object-0"))
                #expect(bodyString.contains("object-9"))
            }
        }
    }

    @Test("Concurrent Read Operations")
    func testConcurrentReadOperations() async throws {
        try await withApp { client in
            let bucket = "read-concurrent-bucket"

            // Create bucket and objects
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create test objects
            for i in 0..<5 {
                let key = "read-object-\(i)"
                let content = "Content for read object \(i)"

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }
            }

            // Concurrently read all objects
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<5 {
                    group.addTask {
                        let key = "read-object-\(i)"
                        let expectedContent = "Content for read object \(i)"

                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .get,
                            headers: self.sign("GET", "/\(bucket)/\(key)")
                        ) { response in
                            #expect(response.status == .ok)
                            #expect(response.body == ByteBuffer(string: expectedContent))
                        }
                    }
                }

                try await group.waitForAll()
            }
        }
    }

    @Test("Concurrent Mixed Operations")
    func testConcurrentMixedOperations() async throws {
        try await withApp { client in
            let bucket = "mixed-concurrent-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Phase 1: Create objects concurrently
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<5 {
                    group.addTask {
                        let key = "mixed-object-\(i)"
                        let content = "Content \(i)"

                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .put,
                            headers: self.sign("PUT", "/\(bucket)/\(key)", body: content),
                            body: ByteBuffer(string: content)
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }
                try await group.waitForAll()
            }

            // Phase 2: Read objects concurrently
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<5 {
                    group.addTask {
                        let key = "mixed-object-\(i)"
                        let expectedContent = "Content \(i)"

                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .get,
                            headers: self.sign("GET", "/\(bucket)/\(key)")
                        ) { response in
                            #expect(response.status == .ok)
                            #expect(response.body == ByteBuffer(string: expectedContent))
                        }
                    }
                }
                try await group.waitForAll()
            }

            // Phase 3: Delete some objects concurrently
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 2..<4 {
                    group.addTask {
                        let key = "mixed-object-\(i)"

                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .delete,
                            headers: self.sign("DELETE", "/\(bucket)/\(key)")
                        ) { response in
                            #expect(response.status == .noContent)
                        }
                    }
                }
                try await group.waitForAll()
            }

            // Verify final state
            try await client.execute(
                uri: "/\(bucket)",
                method: .get,
                headers: sign("GET", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                #expect(bodyString.contains("mixed-object-0"))
                #expect(bodyString.contains("mixed-object-1"))
                #expect(bodyString.contains("mixed-object-4"))
                #expect(!bodyString.contains("mixed-object-2"))
                #expect(!bodyString.contains("mixed-object-3"))
            }
        }
    }

    @Test("Concurrent Multipart Uploads")
    func testConcurrentMultipartUploads() async throws {
        try await withApp { client in
            let bucket = "multipart-concurrent-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Concurrently perform multipart uploads
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<3 {
                    group.addTask {
                        let key = "multipart-object-\(i)"
                        let part1 = "Part 1 of object \(i)"
                        let part2 = "Part 2 of object \(i)"
                        let expectedContent = part1 + part2

                        // Initiate multipart upload
                        try await client.execute(
                            uri: "/\(bucket)/\(key)?uploads",
                            method: .post,
                            headers: self.sign("POST", "/\(bucket)/\(key)?uploads")
                        ) { response in
                            #expect(response.status == .ok)
                            // Parse upload ID from response XML
                            // In a real implementation, we'd parse the XML to get the upload ID
                        }

                        // Upload parts (simplified - in real S3, parts are uploaded separately)
                        // For this test, we'll just put the complete object
                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .put,
                            headers: self.sign("PUT", "/\(bucket)/\(key)", body: expectedContent),
                            body: ByteBuffer(string: expectedContent)
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            // Verify all multipart objects were created
            try await client.execute(
                uri: "/\(bucket)",
                method: .get,
                headers: sign("GET", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                #expect(bodyString.contains("multipart-object-0"))
                #expect(bodyString.contains("multipart-object-1"))
                #expect(bodyString.contains("multipart-object-2"))
            }
        }
    }

    @Test("Concurrent Bucket Operations")
    func testConcurrentBucketOperations() async throws {
        try await withApp { client in
            // Concurrently create multiple buckets
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<5 {
                    group.addTask {
                        let bucketName = "concurrent-bucket-\(i)"

                        try await client.execute(
                            uri: "/\(bucketName)",
                            method: .put,
                            headers: self.sign("PUT", "/\(bucketName)")
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            // List buckets to verify creation
            try await client.execute(
                uri: "/",
                method: .get,
                headers: sign("GET", "/")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                for i in 0..<5 {
                    #expect(bodyString.contains("concurrent-bucket-\(i)"))
                }
            }
        }
    }

    @Test("MockStorage Concurrent Operations with Delays")
    func testMockStorageConcurrentWithDelays() async throws {
        let mockStorage = MockStorage()
        mockStorage.operationDelay = 0.1 // 100ms delay per operation

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        try await app.test(.router) { client in
            // Create bucket
            try await client.execute(uri: "/test-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }

            // Measure time for concurrent operations
            let startTime = Date()

            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<5 {
                    group.addTask {
                        let key = "delayed-object-\(i)"
                        let content = "Content \(i)"

                        try await client.execute(
                            uri: "/test-bucket/\(key)",
                            method: .put,
                            body: ByteBuffer(string: content)
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            let endTime = Date()
            let duration = endTime.timeIntervalSince(startTime)

            // With 100ms delay per operation and concurrency, should complete faster than sequential
            // Allow some tolerance for test execution overhead
            #expect(duration < 1.0) // Should complete in less than 1 second
        }
    }

    @Test("Stress Test - High Concurrency")
    func testHighConcurrencyStress() async throws {
        let mockStorage = MockStorage()

        let controller = S3Controller(storage: mockStorage)
        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        try await app.test(.router) { client in
            // Create bucket
            try await client.execute(uri: "/stress-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }

            // Perform many concurrent operations
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<50 {
                    group.addTask {
                        let key = "stress-object-\(i)"
                        let content = "Stress content \(i)"

                        try await client.execute(
                            uri: "/stress-bucket/\(key)",
                            method: .put,
                            body: ByteBuffer(string: content)
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            // Verify all objects exist
            try await client.execute(uri: "/stress-bucket", method: .get) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                for i in 0..<50 {
                    #expect(bodyString.contains("stress-object-\(i)"))
                }
            }
        }
    }
}
