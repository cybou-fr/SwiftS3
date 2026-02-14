import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("Performance Benchmarks")
struct PerformanceTests {

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

    @Test("Benchmark Object Put Operations")
    func benchmarkObjectPutOperations() async throws {
        try await withApp { client in
            let bucket = "bench-put-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Benchmark small object puts
            let smallContent = "Small content for benchmarking"
            var putTimes: [TimeInterval] = []

            for i in 0..<100 {
                let key = "small-object-\(i)"
                let startTime = Date()

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: smallContent),
                    body: ByteBuffer(string: smallContent)
                ) { response in
                    #expect(response.status == .ok)
                }

                let endTime = Date()
                putTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgPutTime = putTimes.reduce(0, +) / Double(putTimes.count)
            let minPutTime = putTimes.min() ?? 0
            let maxPutTime = putTimes.max() ?? 0

            print("Small Object Put Performance:")
            print("  Average: \(String(format: "%.4f", avgPutTime))s")
            print("  Min: \(String(format: "%.4f", minPutTime))s")
            print("  Max: \(String(format: "%.4f", maxPutTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgPutTime))")

            // Baseline performance assertions
            #expect(avgPutTime < 0.1, "Average put time should be less than 100ms")
            #expect(putTimes.filter { $0 > 1.0 }.count == 0, "No put operation should take more than 1 second")

            // Benchmark medium object puts (10KB)
            let mediumContent = String(repeating: "x", count: 10 * 1024)
            putTimes.removeAll()

            for i in 0..<50 {
                let key = "medium-object-\(i)"
                let startTime = Date()

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: mediumContent),
                    body: ByteBuffer(string: mediumContent)
                ) { response in
                    #expect(response.status == .ok)
                }

                let endTime = Date()
                putTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgMediumPutTime = putTimes.reduce(0, +) / Double(putTimes.count)
            print("Medium Object (10KB) Put Performance:")
            print("  Average: \(String(format: "%.4f", avgMediumPutTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgMediumPutTime))")
        }
    }

    @Test("Benchmark Object Get Operations")
    func benchmarkObjectGetOperations() async throws {
        try await withApp { client in
            let bucket = "bench-get-bucket"

            // Create bucket and objects
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create test objects
            let testContent = "Benchmark test content"
            for i in 0..<100 {
                let key = "bench-object-\(i)"
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: testContent),
                    body: ByteBuffer(string: testContent)
                ) { response in
                    #expect(response.status == .ok)
                }
            }

            // Benchmark get operations
            var getTimes: [TimeInterval] = []

            for i in 0..<100 {
                let key = "bench-object-\(i)"
                let startTime = Date()

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .get,
                    headers: sign("GET", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .ok)
                    #expect(response.body == ByteBuffer(string: testContent))
                }

                let endTime = Date()
                getTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgGetTime = getTimes.reduce(0, +) / Double(getTimes.count)
            let minGetTime = getTimes.min() ?? 0
            let maxGetTime = getTimes.max() ?? 0

            print("Object Get Performance:")
            print("  Average: \(String(format: "%.4f", avgGetTime))s")
            print("  Min: \(String(format: "%.4f", minGetTime))s")
            print("  Max: \(String(format: "%.4f", maxGetTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgGetTime))")
        }
    }

    @Test("Benchmark List Operations")
    func benchmarkListOperations() async throws {
        try await withApp { client in
            let bucket = "bench-list-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create many objects
            for i in 0..<1000 {
                let key = "list-object-\(String(format: "%04d", i))"
                let content = "Content \(i)"
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }
            }

            // Benchmark list operations
            var listTimes: [TimeInterval] = []

            for _ in 0..<10 {
                let startTime = Date()

                try await client.execute(
                    uri: "/\(bucket)",
                    method: .get,
                    headers: sign("GET", "/\(bucket)")
                ) { response in
                    #expect(response.status == .ok)
                    let bodyString = String(buffer: response.body)
                    #expect(bodyString.contains("list-object-0000"))
                    #expect(bodyString.contains("list-object-0999"))
                }

                let endTime = Date()
                listTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgListTime = listTimes.reduce(0, +) / Double(listTimes.count)
            print("List Objects Performance (1000 objects):")
            print("  Average: \(String(format: "%.4f", avgListTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgListTime))")

            // Benchmark with pagination
            var paginatedListTimes: [TimeInterval] = []

            for _ in 0..<10 {
                let startTime = Date()

                try await client.execute(
                    uri: "/\(bucket)?max-keys=100",
                    method: .get,
                    headers: sign("GET", "/\(bucket)?max-keys=100")
                ) { response in
                    #expect(response.status == .ok)
                }

                let endTime = Date()
                paginatedListTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgPaginatedListTime = paginatedListTimes.reduce(0, +) / Double(paginatedListTimes.count)
            print("Paginated List Objects Performance (max-keys=100):")
            print("  Average: \(String(format: "%.4f", avgPaginatedListTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgPaginatedListTime))")
        }
    }

    @Test("Benchmark Concurrent Operations")
    func benchmarkConcurrentOperations() async throws {
        try await withApp { client in
            let bucket = "bench-concurrent-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Benchmark concurrent puts
            let startTime = Date()

            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<100 {
                    group.addTask {
                        let key = "concurrent-object-\(i)"
                        let content = "Concurrent content \(i)"

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

            let endTime = Date()
            let totalTime = endTime.timeIntervalSince(startTime)
            let avgTimePerOperation = totalTime / 100.0

            print("Concurrent Put Performance (100 operations):")
            print("  Total time: \(String(format: "%.4f", totalTime))s")
            print("  Average per operation: \(String(format: "%.4f", avgTimePerOperation))s")
            print("  Operations/sec: \(String(format: "%.2f", 100.0 / totalTime))")

            // Benchmark concurrent gets
            let getStartTime = Date()

            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<100 {
                    group.addTask {
                        let key = "concurrent-object-\(i)"
                        let expectedContent = "Concurrent content \(i)"

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

            let getEndTime = Date()
            let getTotalTime = getEndTime.timeIntervalSince(getStartTime)
            let getAvgTimePerOperation = getTotalTime / 100.0

            print("Concurrent Get Performance (100 operations):")
            print("  Total time: \(String(format: "%.4f", getTotalTime))s")
            print("  Average per operation: \(String(format: "%.4f", getAvgTimePerOperation))s")
            print("  Operations/sec: \(String(format: "%.2f", 100.0 / getTotalTime))")
        }
    }

    @Test("Benchmark Storage Backend Operations")
    func benchmarkStorageBackendOperations() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        
        do {
            try await storage.createBucket(name: "bench-storage-bucket", owner: "test-owner")

            // Benchmark direct storage operations
            let content = ByteBuffer(string: "Benchmark content")

            // Put operations
            var putTimes: [TimeInterval] = []
            for i in 0..<100 {
                let key = "storage-object-\(i)"
                let startTime = Date()

                _ = try await storage.putObject(
                    bucket: "bench-storage-bucket",
                    key: key,
                    data: [content].async,
                    size: Int64(content.readableBytes),
                    metadata: nil,
                    owner: "test-owner"
                )

                let endTime = Date()
                putTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgPutTime = putTimes.reduce(0, +) / Double(putTimes.count)
            print("Direct Storage Put Performance:")
            print("  Average: \(String(format: "%.4f", avgPutTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgPutTime))")

            // Get operations
            var getTimes: [TimeInterval] = []
            for i in 0..<100 {
                let key = "storage-object-\(i)"
                let startTime = Date()

                _ = try await storage.getObject(bucket: "bench-storage-bucket", key: key, versionId: nil, range: nil)

                let endTime = Date()
                getTimes.append(endTime.timeIntervalSince(startTime))
            }

            let avgGetTime = getTimes.reduce(0, +) / Double(getTimes.count)
            print("Direct Storage Get Performance:")
            print("  Average: \(String(format: "%.4f", avgGetTime))s")
            print("  Operations/sec: \(String(format: "%.2f", 1.0 / avgGetTime))")

            // List operations
            let listStartTime = Date()
            _ = try await storage.listObjects(bucket: "bench-storage-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil, maxKeys: nil)
            let listEndTime = Date()
            let listTime = listEndTime.timeIntervalSince(listStartTime)

            print("Direct Storage List Performance (100 objects):")
            print("  Time: \(String(format: "%.4f", listTime))s")
        } catch {
            // Cleanup on error
            try? await storage.shutdown()
            try? FileManager.default.removeItem(atPath: root)
            throw error
        }
        
        // Cleanup
        try? await storage.shutdown()
        try? FileManager.default.removeItem(atPath: root)
    }

    @Test("Memory Usage Benchmark")
    func benchmarkMemoryUsage() async throws {
        let mockStorage = MockStorage()
        let controller = S3Controller(storage: mockStorage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(MockAuthenticatorMiddleware())
        controller.addRoutes(to: router)

        let app = Application(router: router, configuration: .init(address: .hostname("127.0.0.1", port: 0)))

        try await app.test(.router) { client in
            // Create bucket
            try await client.execute(uri: "/memory-bucket", method: .put) { response in
                #expect(response.status == .ok)
            }

            // Create objects of varying sizes to test memory usage
            let sizes = [1, 1024, 10*1024, 100*1024, 1024*1024] // 1B, 1KB, 10KB, 100KB, 1MB

            for size in sizes {
                let content = String(repeating: "x", count: size)
                let key = "memory-object-\(size)bytes"

                let startTime = Date()
                try await client.execute(
                    uri: "/memory-bucket/\(key)",
                    method: .put,
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }
                let endTime = Date()

                print("Memory benchmark - \(size) bytes:")
                print("  Upload time: \(String(format: "%.4f", endTime.timeIntervalSince(startTime)))s")

                // Test retrieval
                let getStartTime = Date()
                try await client.execute(uri: "/memory-bucket/\(key)", method: .get) { response in
                    #expect(response.status == .ok)
                    #expect(response.body.readableBytes == size)
                }
                let getEndTime = Date()

                print("  Download time: \(String(format: "%.4f", getEndTime.timeIntervalSince(getStartTime)))s")
            }
        }
    }
}
