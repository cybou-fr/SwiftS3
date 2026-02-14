import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("Stress Testing and Load Validation")
struct StressTests {

    func withApp(_ test: @escaping @Sendable (any TestClientProtocol) async throws -> Void)
        async throws
    {
        // Create per-test event loop group and thread pool
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
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

    @Test("High Volume Object Creation")
    func testHighVolumeObjectCreation() async throws {
        try await withApp { client in
            let bucket = "stress-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: self.sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create 100 objects
            for i in 0..<100 {
                let key = "stress-object-\(String(format: "%03d", i))"
                let content = "Content for stress test object \(i)"

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: self.sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }
            }

            // Verify count via list operation
            try await client.execute(
                uri: "/\(bucket)",
                method: .get,
                headers: self.sign("GET", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                // Should contain at least some of our objects
                #expect(bodyString.contains("stress-object-000"))
                #expect(bodyString.contains("stress-object-099"))
            }
        }
    }

    @Test("Memory Usage Under Load")
    func testMemoryUsageUnderLoad() async throws {
        try await withApp { client in
            let bucket = "memory-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: self.sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create objects of varying sizes to test memory handling
            let sizes = [1, 10, 100, 1000, 10000] // bytes

            for size in sizes {
                let content = String(repeating: "x", count: size)
                let key = "memory-test-\(size)bytes"

                let startTime = Date()
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: self.sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }
                let endTime = Date()

                print("Memory test - \(size) bytes: upload took \(String(format: "%.4f", endTime.timeIntervalSince(startTime)))s")

                // Test retrieval
                let getStartTime = Date()
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .get,
                    headers: self.sign("GET", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .ok)
                    #expect(response.body.readableBytes == size)
                }
                let getEndTime = Date()

                print("Memory test - \(size) bytes: download took \(String(format: "%.4f", getEndTime.timeIntervalSince(getStartTime)))s")
            }
        }
    }

    @Test("Concurrent Bucket Operations")
    func testConcurrentBucketOperations() async throws {
        try await withApp { client in
            // Create multiple buckets concurrently
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

                        // Add an object to each bucket
                        let key = "object-in-bucket-\(i)"
                        let content = "Content in bucket \(i)"

                        try await client.execute(
                            uri: "/\(bucketName)/\(key)",
                            method: .put,
                            headers: self.sign("PUT", "/\(bucketName)/\(key)", body: content),
                            body: ByteBuffer(string: content)
                        ) { response in
                            #expect(response.status == .ok)
                        }
                    }
                }

                try await group.waitForAll()
            }

            // Verify all buckets exist
            for i in 0..<5 {
                let bucketName = "concurrent-bucket-\(i)"
                try await client.execute(
                    uri: "/\(bucketName)",
                    method: .get,
                    headers: self.sign("GET", "/\(bucketName)")
                ) { response in
                    #expect(response.status == .ok)
                }
            }
        }
    }

    @Test("Rapid Create/Delete Cycles")
    func testRapidCreateDeleteCycles() async throws {
        try await withApp { client in
            let bucket = "cycle-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: self.sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Perform rapid create/delete cycles
            for cycle in 0..<10 {
                let key = "cycle-object-\(cycle)"
                let content = "Content for cycle \(cycle)"

                // Create object
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: self.sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }

                // Verify it exists
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .get,
                    headers: self.sign("GET", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .ok)
                    #expect(String(buffer: response.body) == content)
                }

                // Delete object
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .delete,
                    headers: self.sign("DELETE", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .noContent || response.status == .ok)
                }

                // Verify it's gone
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .get,
                    headers: sign("GET", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .notFound)
                }
            }
        }
    }

    @Test("Large Number of Small Objects")
    func testLargeNumberOfSmallObjects() async throws {
        try await withApp { client in
            let bucket = "many-objects-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create 500 small objects
            let startTime = Date()
            for i in 0..<500 {
                let key = "small-object-\(String(format: "%03d", i))"
                let content = "data\(i)"

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }
            }
            let endTime = Date()

            print("Created 500 small objects in \(String(format: "%.2f", endTime.timeIntervalSince(startTime))) seconds")

            // List objects to verify
            try await client.execute(
                uri: "/\(bucket)?max-keys=1000",
                method: .get,
                headers: sign("GET", "/\(bucket)?max-keys=1000")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                // Should contain many objects
                #expect(bodyString.contains("small-object-000"))
                #expect(bodyString.contains("small-object-499"))
            }
        }
    }
}