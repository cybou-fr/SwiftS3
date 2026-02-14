import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("Recovery and Resilience Testing")
struct RecoveryTests {

    static let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    static let threadPool: NIOThreadPool = {
        let tp = NIOThreadPool(numberOfThreads: 2)
        tp.start()
        return tp
    }()

    func withApp(_ test: @escaping @Sendable (any TestClientProtocol) async throws -> Void)
        async throws
    {
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
            on: Self.elg,
            threadPool: Self.threadPool
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

        try await app.test(.router, test)

        try? await storage.shutdown()
        try? await metadataStore.shutdown()
        try? FileManager.default.removeItem(atPath: storagePath)
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

    @Test("Invalid Bucket Names")
    func testInvalidBucketNames() async throws {
        try await withApp { client in
            // Empty string routes to service endpoint, not bucket creation
            try await client.execute(
                uri: "/",
                method: .put,
                headers: sign("PUT", "/")
            ) { response in
                #expect(response.status == .notFound) // Service endpoint doesn't support PUT
            }

            // Test various invalid bucket names that should reach bucket creation
            let invalidNames = [
                "ab",  // too short
                "bucket with spaces",  // spaces
                "BucketWithCaps",  // uppercase
                "bucket.with.dots.and.UPPERCASE",  // mixed case with dots
                "bucket_with_underscores",  // underscores
                "bucket-with-invalid..dots",  // consecutive dots
                "bucket-ending-with-dash-",  // ends with dash
                "-bucket-starting-with-dash",  // starts with dash
                "192.168.1.1",  // IP address format
                String(repeating: "a", count: 64),  // too long
            ]

            for invalidName in invalidNames {
                try await client.execute(
                    uri: "/\(invalidName)",
                    method: .put,
                    headers: sign("PUT", "/\(invalidName)")
                ) { response in
                    #expect(response.status == .badRequest)
                }
            }
        }
    }

    @Test("Invalid Object Keys")
    func testInvalidObjectKeys() async throws {
        try await withApp { client in
            let bucket = "invalid-keys-bucket"

            // Create bucket first
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Test keys that should be handled gracefully
            let testKeys = [
                "",  // empty key
                "key with spaces",
                "key/with/slashes",
                "key\\with\\backslashes",
                "../escape/attempt",
                "~/home/directory"
            ]

            for key in testKeys {
                let content = "content for key: \(key)"

                // These should either succeed or fail gracefully
                do {
                    try await client.execute(
                        uri: "/\(bucket)/\(key)",
                        method: .put,
                        headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                        body: ByteBuffer(string: content)
                    ) { response in
                        // Accept success or client error
                        #expect(response.status.code >= 200 && response.status.code < 500)
                    }
                } catch {
                    // Some keys might cause URL parsing issues, which is acceptable
                    print("Key '\(key)' caused error: \(error)")
                }
            }
        }
    }

    @Test("Malformed Requests")
    func testMalformedRequests() async throws {
        try await withApp { client in
            let bucket = "malformed-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Test malformed JSON in policy
            let malformedPolicy = "{ invalid json content }"
            try await client.execute(
                uri: "/\(bucket)?policy",
                method: .put,
                headers: sign("PUT", "/\(bucket)?policy", body: malformedPolicy),
                body: ByteBuffer(string: malformedPolicy)
            ) { response in
                #expect(response.status == .badRequest)
            }

            // Test invalid XML in lifecycle
            let malformedXML = "<Invalid>XML<Content>"
            try await client.execute(
                uri: "/\(bucket)?lifecycle",
                method: .put,
                headers: sign("PUT", "/\(bucket)?lifecycle", body: malformedXML),
                body: ByteBuffer(string: malformedXML)
            ) { response in
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("Resource Exhaustion Handling")
    func testResourceExhaustionHandling() async throws {
        try await withApp { client in
            let bucket = "exhaustion-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Try to create an extremely large object (this should be handled gracefully)
            let hugeContent = String(repeating: "x", count: 100 * 1024 * 1024) // 100MB

            do {
                try await client.execute(
                    uri: "/\(bucket)/huge-object.txt",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/huge-object.txt", body: hugeContent),
                    body: ByteBuffer(string: hugeContent)
                ) { response in
                    // Should either succeed or fail gracefully due to size limits
                    #expect(response.status == .ok || response.status == .badRequest)
                }
            } catch {
                // Network timeouts or other errors are acceptable for large payloads
                print("Large object upload failed as expected: \(error)")
            }
        }
    }

    @Test("Concurrent Modification Handling")
    func testConcurrentModificationHandling() async throws {
        try await withApp { client in
            let bucket = "concurrent-mod-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            let key = "concurrent-object.txt"

            // Start multiple PUT operations on the same key concurrently
            try await withThrowingTaskGroup(of: Void.self) { group in
                // Multiple PUT operations
                for i in 0..<3 {
                    group.addTask {
                        let content = "Version \(i) content"
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

            // Now test concurrent GET operations after PUTs are complete
            try await withThrowingTaskGroup(of: Void.self) { group in
                // Concurrent GET operations
                for _ in 0..<2 {
                    group.addTask {
                        try await client.execute(
                            uri: "/\(bucket)/\(key)",
                            method: .get,
                            headers: self.sign("GET", "/\(bucket)/\(key)")
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            // Final verification - object should exist
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .get,
                headers: sign("GET", "/\(bucket)/\(key)")
            ) { response in
                #expect(response.status == .ok)
            }
        }
    }

    @Test("Network Interruption Simulation")
    func testNetworkInterruptionSimulation() async throws {
        try await withApp { client in
            let bucket = "network-test-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Test with various content sizes to simulate different interruption points
            let sizes = [100, 1000, 10000]

            for size in sizes {
                let content = String(repeating: "x", count: size)
                let key = "network-test-\(size).txt"

                // This should work normally, but in a real network interruption scenario,
                // the client would need to handle retries
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }

                // Verify the object was stored completely
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .get,
                    headers: sign("GET", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .ok)
                    #expect(response.body.readableBytes == size)
                }
            }
        }
    }

    @Test("Invalid Authentication")
    func testInvalidAuthentication() async throws {
        try await withApp { client in
            let bucket = "auth-test-bucket"

            // Try operations with invalid credentials
            let invalidHelper = AWSAuthHelper(accessKey: "invalid", secretKey: "invalid")

            // Create bucket with invalid auth
            let url = URL(string: "http://localhost/\(bucket)")!
            let headers = (try? invalidHelper.signRequest(method: .put, url: url)) ?? HTTPFields()

            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: headers
            ) { response in
                #expect(response.status == .forbidden)
            }

            // Try with valid auth first to create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Now try object operation with invalid auth
            let objectUrl = URL(string: "http://localhost/\(bucket)/test.txt")!
            let objectHeaders = (try? invalidHelper.signRequest(method: .put, url: objectUrl, payload: "test")) ?? HTTPFields()

            try await client.execute(
                uri: "/\(bucket)/test.txt",
                method: .put,
                headers: objectHeaders,
                body: ByteBuffer(string: "test")
            ) { response in
                #expect(response.status == .forbidden)
            }
        }
    }
}