import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO
import Testing

@testable import SwiftS3

@Suite("SwiftS3 Tests")
struct SwiftS3Tests {

    // MARK: - Helper
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

        // Ensure storage directory exists
        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        // Initialize SQL Metadata Store
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

    // MARK: - Storage Tests

    @Test("Storage: ListObjects Pagination & Filtering")
    func testStorageListObjectsPagination() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "list-bucket", owner: "test-owner")

        // Create nested structure
        // a/1.txt
        // a/2.txt
        // b/1.txt
        // c.txt
        let data = ByteBuffer(string: ".")
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "a/1.txt", data: [data].async, size: 1, metadata: nil,
            owner: "test-owner")
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "a/2.txt", data: [data].async, size: 1, metadata: nil,
            owner: "test-owner")
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "b/1.txt", data: [data].async, size: 1, metadata: nil,
            owner: "test-owner")
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "c.txt", data: [data].async, size: 1, metadata: nil,
            owner: "test-owner")

        // 1. Prefix
        let res1 = try await storage.listObjects(
            bucket: "list-bucket", prefix: "a/", delimiter: nil, marker: nil,
            continuationToken: nil, maxKeys: 1000)
        #expect(res1.objects.count == 2)
        #expect(res1.objects[0].key == "a/1.txt")
        #expect(res1.objects[1].key == "a/2.txt")

        // 2. Delimiter (folders)
        let res2 = try await storage.listObjects(
            bucket: "list-bucket", prefix: nil, delimiter: "/", marker: nil, continuationToken: nil,
            maxKeys: 1000)
        #expect(res2.objects.count == 1)  // c.txt
        #expect(res2.objects[0].key == "c.txt")
        #expect(res2.commonPrefixes.count == 2)  // a/, b/
        #expect(res2.commonPrefixes.contains("a/"))
        #expect(res2.commonPrefixes.contains("b/"))

        // 3. Pagination (MaxKeys)
        let res3 = try await storage.listObjects(
            bucket: "list-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil,
            maxKeys: 2)
        #expect(res3.objects.count == 2)
        #expect(res3.isTruncated == true)
        #expect(res3.nextMarker == "a/2.txt")

        // 4. Pagination (Marker)
        let res4 = try await storage.listObjects(
            bucket: "list-bucket", prefix: nil, delimiter: nil, marker: "a/2.txt",
            continuationToken: nil, maxKeys: 1000)
        #expect(res4.objects.count == 2)
        #expect(res4.objects[0].key == "b/1.txt")
        #expect(res4.objects[1].key == "c.txt")

        // Cleanup
        try? await storage.shutdown()
    }

    @Test("Storage: Copy Object")
    func testStorageCopyObject() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        )
        .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "src-bucket", owner: "test-owner")
        try await storage.createBucket(name: "dst-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Copy Me")
        let metadata = ["x-amz-meta-original": "true"]
        _ = try await storage.putObject(
            bucket: "src-bucket", key: "source.txt", data: [data].async,
            size: Int64(data.readableBytes), metadata: metadata, owner: "test-owner")

        // Copy
        let copyMeta = try await storage.copyObject(
            fromBucket: "src-bucket", fromKey: "source.txt", toBucket: "dst-bucket",
            toKey: "copied.txt", owner: "test-owner")

        #expect(copyMeta.key == "copied.txt")
        #expect(copyMeta.size == Int64(data.readableBytes))
        #expect(copyMeta.customMetadata["x-amz-meta-original"] == "true")

        // Verify content
        let (_, body) = try await storage.getObject(
            bucket: "dst-bucket", key: "copied.txt", versionId: nil, range: nil)
        var received = ""
        if let body = body {
            for await chunk in body {
                received += String(buffer: chunk)
            }
        }
        #expect(received == "Copy Me")

        // Cleanup
        try? await storage.shutdown()
    }

    @Test("Storage: Create and Delete Bucket")
    func testStorageCreateDeleteBucket() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "test-bucket", owner: "test-owner")
        let result = try await storage.listObjects(
            bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil,
            maxKeys: nil)
        #expect(result.objects.count == 0)

        try await storage.deleteBucket(name: "test-bucket")
        let bucketsAfter = try await storage.listBuckets()
        #expect(bucketsAfter.isEmpty)

        // Cleanup
        try? await storage.shutdown()
    }

    @Test("Storage: Put and Get Object")
    func testStoragePutGetObject() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = Data("Hello, World!".utf8)
        let buffer = ByteBuffer(bytes: data)
        let stream = AsyncStream<ByteBuffer> { continuation in
            continuation.yield(buffer)
            continuation.finish()
        }

        let meta = try await storage.putObject(
            bucket: "test-bucket", key: "hello.txt", data: stream, size: Int64(data.count),
            metadata: nil, owner: "test-owner")
        #expect(meta.eTag != nil && !meta.eTag!.isEmpty)

        let result = try await storage.listObjects(
            bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil,
            maxKeys: nil)
        #expect(result.objects.count == 1)
        #expect(result.objects.first?.key == "hello.txt")

        let (metadata, body) = try await storage.getObject(
            bucket: "test-bucket", key: "hello.txt", versionId: nil, range: nil)
        #expect(metadata.size == Int64(data.count))

        var receivedData = Data()
        if let body = body {
            for await chunk in body {
                receivedData.append(contentsOf: chunk.readableBytesView)
            }
        }
        #expect(receivedData == data)

        // Cleanup
        try? await storage.shutdown()
    }

    @Test("Storage: S3 Select")
    func testStorageS3Select() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let csvData = "name,age\nAlice,30\nBob,25\n"
        let buffer = ByteBuffer(string: csvData)
        let stream = AsyncStream<ByteBuffer> { continuation in
            continuation.yield(buffer)
            continuation.finish()
        }

        let meta = try await storage.putObject(
            bucket: "test-bucket", key: "data.csv", data: stream, size: Int64(csvData.count),
            metadata: nil, owner: "test-owner")
        #expect(meta.eTag != nil && !meta.eTag!.isEmpty)

        // For S3 Select, we test the logic indirectly since it's in the controller
        // In a full test, we'd call the POST endpoint with select query
        // For now, just ensure the object exists
        let (metadata, _) = try await storage.getObject(
            bucket: "test-bucket", key: "data.csv", versionId: nil, range: nil)
        #expect(metadata.size == Int64(csvData.count))

        // Cleanup
        try? await storage.shutdown()
    }

    // MARK: - API Tests

    @Test("API: List Buckets (No Auth)")
    func testAPIListBucketsNoAuth() async throws {
        try await withApp { app in
            try await app.execute(uri: "/", method: HTTPTypes.HTTPRequest.Method.get) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body.contains("<ListAllMyBucketsResult>"))
            }
        }
    }

    @Test("API: Configurable Authentication")
    func testAPIConfigurableAuth() async throws {
        // Create per-test event loop group and thread pool
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path

        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite",
            on: elg,
            threadPool: threadPool
        )

        let customAccess = "user123"
        let customSecret = "secret456"
        try await metadataStore.createUser(
            username: "custom", accessKey: customAccess, secretKey: customSecret)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(userStore: metadataStore))
        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)
        S3Controller(storage: storage).addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname("127.0.0.1", port: 0))
        )

        do {
            try await app.test(.router) { client in
                let helper = AWSAuthHelper(accessKey: customAccess, secretKey: customSecret)
                let url = URL(string: "http://localhost:8080/")!
                let headers = try helper.signRequest(method: .get, url: url)

                try await client.execute(uri: "/", method: .get, headers: headers) { response in
                    #expect(response.status == .ok)
                }

                // Test with WRONG credentials
                let wrongHelper = AWSAuthHelper(accessKey: "wrong", secretKey: "credentials")
                let wrongHeaders = try wrongHelper.signRequest(method: .get, url: url)

                try await client.execute(uri: "/", method: .get, headers: wrongHeaders) { response in
                    #expect(response.status == .forbidden)
                }
            }
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

    @Test("API: List Objects V2")
    func testAPIListObjectsV2() async throws {
        try await withApp { app in
            // Create Bucket
            let helper = AWSAuthHelper()
            let bucketUrl = URL(string: "http://localhost:8080/v2-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/v2-bucket", method: .put, headers: createHeaders) { _ in }

            // Put Objects
            for i in 1...3 {
                let objectUrl = URL(string: "http://localhost:8080/v2-bucket/obj\(i)")!
                let headers = try helper.signRequest(method: .put, url: objectUrl, payload: "data")
                try await app.execute(
                    uri: "/v2-bucket/obj\(i)", method: .put, headers: headers,
                    body: ByteBuffer(string: "data")
                ) { _ in }
            }

            // List V2
            let listUrl = URL(string: "http://localhost:8080/v2-bucket?list-type=2")!
            let listHeaders = try helper.signRequest(method: .get, url: listUrl)
            try await app.execute(uri: "/v2-bucket?list-type=2", method: .get, headers: listHeaders)
            {
                response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body.contains("<ListBucketResult"))
                #expect(body.contains("<KeyCount>3</KeyCount>"))
                #expect(body.contains("<Key>obj1</Key>"))
            }
        }
    }

    @Test("API: Delete Objects (Bulk Delete)")
    func testAPIDeleteObjects() async throws {
        try await withApp { app in
            // Create Bucket
            let helper = AWSAuthHelper()
            let bucketUrl = URL(string: "http://localhost:8080/delete-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/delete-bucket", method: .put, headers: createHeaders) {
                _ in
            }

            // Put Objects
            let keys = ["obj1", "obj2", "obj3"]
            for key in keys {
                let objectUrl = URL(string: "http://localhost:8080/delete-bucket/\(key)")!
                let headers = try helper.signRequest(method: .put, url: objectUrl, payload: "data")
                try await app.execute(
                    uri: "/delete-bucket/\(key)", method: .put, headers: headers,
                    body: ByteBuffer(string: "data")
                ) { _ in }
            }

            // Verify they exist
            let listUrl = URL(string: "http://localhost:8080/delete-bucket")!
            let listHeaders = try helper.signRequest(method: .get, url: listUrl)
            try await app.execute(uri: "/delete-bucket", method: .get, headers: listHeaders) {
                response in
                let body = String(buffer: response.body)
                #expect(body.contains("obj1"))
                #expect(body.contains("obj2"))
                #expect(body.contains("obj3"))
            }

            // Bulk Delete
            let deleteXml = """
                <Delete>
                    <Object><Key>obj1</Key></Object>
                    <Object><Key>obj2</Key></Object>
                </Delete>
                """
            let bulkDeleteUrl = URL(string: "http://localhost:8080/delete-bucket?delete")!
            let deleteHeaders = try helper.signRequest(
                method: .post, url: bulkDeleteUrl, payload: deleteXml)
            try await app.execute(
                uri: "/delete-bucket?delete", method: .post, headers: deleteHeaders,
                body: ByteBuffer(string: deleteXml)
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body.contains("<Deleted>"))
                #expect(body.contains("<Key>obj1</Key>"))
                #expect(body.contains("<Key>obj2</Key>"))
            }

            // Verify they are gone
            try await app.execute(uri: "/delete-bucket", method: .get, headers: listHeaders) {
                response in
                let body = String(buffer: response.body)
                #expect(!body.contains("obj1"))
                #expect(!body.contains("obj2"))
                #expect(body.contains("obj3"))
            }
        }
    }

    @Test("API: Create Bucket (Auth Required - Fail)")
    func testAPICreateBucketAuthFail() async throws {
        try await withApp { app in
            try await app.execute(
                uri: "/mybucket", method: HTTPTypes.HTTPRequest.Method.put,
                headers: [HTTPField.Name.authorization: "AWS4-HMAC-SHA256 BADHEADER"]
            ) { response in
                #expect(response.status != HTTPResponse.Status.ok)
            }
        }
    }

    @Test("API: List Buckets (Auth Success)")
    func testAPIListBucketsAuthSuccess() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()
            let url = URL(string: "http://localhost:8080/")!
            let headers = try helper.signRequest(method: .get, url: url)

            try await app.execute(uri: "/", method: .get, headers: headers) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body.contains("<ListAllMyBucketsResult>"))
            }
        }
    }

    @Test("API: Put Object (Auth Success)")
    func testAPIPutObjectAuthSuccess() async throws {
        try await withApp { app in
            // 1. Create Bucket first (auth optional/required but let's assume we can do it)
            // For simplicity in this test, we might fallback to storage directly to prep environment?
            // BUT tests usually run in isolation.
            // Let's use valid auth to create bucket too.
            let helper = AWSAuthHelper()

            // Create Bucket
            let bucketUrl = URL(string: "http://localhost:8080/auth-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/auth-bucket", method: .put, headers: createHeaders) {
                response in
                #expect(response.status == .ok)
            }

            // Put Object
            let objectUrl = URL(string: "http://localhost:8080/auth-bucket/test.txt")!
            let content = "Auth Content"
            let putHeaders = try helper.signRequest(method: .put, url: objectUrl, payload: content)

            try await app.execute(
                uri: "/auth-bucket/test.txt", method: .put, headers: putHeaders,
                body: ByteBuffer(string: content)
            ) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.eTag] != nil)
            }
        }
    }

    @Test("API: S3 Select")
    func testAPIS3Select() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()

            // Create Bucket
            let bucketUrl = URL(string: "http://localhost:8080/select-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/select-bucket", method: .put, headers: createHeaders) { _ in }

            // Put CSV Object
            let objectUrl = URL(string: "http://localhost:8080/select-bucket/data.csv")!
            let csvContent = "name,age\nAlice,30\nBob,25\n"
            let putHeaders = try helper.signRequest(method: .put, url: objectUrl, payload: csvContent)
            try await app.execute(
                uri: "/select-bucket/data.csv", method: .put, headers: putHeaders,
                body: ByteBuffer(string: csvContent)
            ) { _ in }

            // S3 Select Query
            let selectUrl = URL(string: "http://localhost:8080/select-bucket/data.csv?select&select-type=2")!
            let selectBody = """
            {
                "Expression": "SELECT * FROM S3Object",
                "ExpressionType": "SQL",
                "InputSerialization": {"CSV": {"FileHeaderInfo": "Use"}},
                "OutputSerialization": {"CSV": {}}
            }
            """
            let selectHeaders = try helper.signRequest(method: .post, url: selectUrl, payload: selectBody)
            try await app.execute(
                uri: "/select-bucket/data.csv?select&select-type=2", method: .post, headers: selectHeaders,
                body: ByteBuffer(string: selectBody)
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body == csvContent)
            }
        }
    }

    @Test("API: Upload Part Copy")
    func testAPIUploadPartCopy() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()

            // Create Bucket
            let bucketUrl = URL(string: "http://localhost:8080/copy-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/copy-bucket", method: .put, headers: createHeaders) { _ in }

            // Put Source Object
            let sourceUrl = URL(string: "http://localhost:8080/copy-bucket/source.txt")!
            let sourceContent = "This is the source content for copy part"
            let putHeaders = try helper.signRequest(method: .put, url: sourceUrl, payload: sourceContent)
            try await app.execute(
                uri: "/copy-bucket/source.txt", method: .put, headers: putHeaders,
                body: ByteBuffer(string: sourceContent)
            ) { _ in }

            // Initiate Multipart Upload
            let initUrl = URL(string: "http://localhost:8080/copy-bucket/dest.txt?uploads")!
            let initHeaders = try helper.signRequest(method: .post, url: initUrl)
            var uploadId = ""
            try await app.execute(uri: "/copy-bucket/dest.txt?uploads", method: .post, headers: initHeaders) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                // Parse upload ID from XML
                if let range = body.range(of: "<UploadId>") {
                    let start = body.index(range.upperBound, offsetBy: 0)
                    if let endRange = body.range(of: "</UploadId>", range: start..<body.endIndex) {
                        uploadId = String(body[start..<endRange.lowerBound])
                    }
                }
                #expect(!uploadId.isEmpty)
            }

            // Upload Part Copy
            let copyUrl = URL(string: "http://localhost:8080/copy-bucket/dest.txt?partNumber=1&uploadId=\(uploadId)")!
            var copyHeaders = try helper.signRequest(method: .put, url: copyUrl)
            copyHeaders[HTTPField.Name("x-amz-copy-source")!] = "/copy-bucket/source.txt"
            try await app.execute(uri: "/copy-bucket/dest.txt?partNumber=1&uploadId=\(uploadId)", method: .put, headers: copyHeaders) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.eTag] != nil)
            }

            // Complete Multipart Upload
            let completeUrl = URL(string: "http://localhost:8080/copy-bucket/dest.txt?uploadId=\(uploadId)")!
            let completeBody = """
            <CompleteMultipartUpload>
                <Part>
                    <ETag>"dummy"</ETag>
                    <PartNumber>1</PartNumber>
                </Part>
            </CompleteMultipartUpload>
            """
            let completeHeaders = try helper.signRequest(method: .post, url: completeUrl, payload: completeBody)
            try await app.execute(
                uri: "/copy-bucket/dest.txt?uploadId=\(uploadId)", method: .post, headers: completeHeaders,
                body: ByteBuffer(string: completeBody)
            ) { response in
                #expect(response.status == .ok)
            }

            // Get the completed object
            let getUrl = URL(string: "http://localhost:8080/copy-bucket/dest.txt")!
            let getHeaders = try helper.signRequest(method: .get, url: getUrl)
            try await app.execute(uri: "/copy-bucket/dest.txt", method: .get, headers: getHeaders) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body == sourceContent)
            }
        }
    }

    @Test("API: Error Handling (404)")
    func testAPI_ErrorHandling() async throws {
        try await withApp { app in
            try await app.execute(uri: "/non-existent-bucket/key", method: .get) { response in
                // Expect 404 Not Found
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("Storage: Metadata Persistence")
    func testStorageMetadata() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "meta-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello Metadata")
        let metadata = ["x-amz-meta-custom": "value123", "Content-Type": "application/json"]

        _ = try await storage.putObject(
            bucket: "meta-bucket", key: "obj", data: [data].async, size: Int64(data.readableBytes),
            metadata: metadata, owner: "test-owner")

        let (readMeta, _) = try await storage.getObject(
            bucket: "meta-bucket", key: "obj", versionId: nil, range: nil)

        #expect(readMeta.contentType == "application/json")
        #expect(readMeta.customMetadata["x-amz-meta-custom"] == "value123")

        // head object
        let headMeta = try await storage.getObjectMetadata(
            bucket: "meta-bucket", key: "obj", versionId: nil)
        #expect(headMeta.contentType == "application/json")
        #expect(headMeta.customMetadata["x-amz-meta-custom"] == "value123")

        // Cleanup
        try? await storage.shutdown()
    }

    @Test("Storage: Range Requests")
    func testStorageRange() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "range-bucket", owner: "test-owner")

        let content = "0123456789"
        let data = ByteBuffer(string: content)
        _ = try await storage.putObject(
            bucket: "range-bucket", key: "digits", data: [data].async,
            size: Int64(data.readableBytes), metadata: nil, owner: "test-owner")

        // Test Range: 0-4 (5 bytes) -> "01234"
        let range1 = ValidatedRange(start: 0, end: 4)
        let (_, body1) = try await storage.getObject(
            bucket: "range-bucket", key: "digits", versionId: nil, range: range1)

        var received1 = ""
        if let body1 = body1 {
            for await chunk in body1 {
                received1 += String(buffer: chunk)
            }
        }
        #expect(received1 == "01234")

        // Test Range: 5-9 (5 bytes) -> "56789"
        let range2 = ValidatedRange(start: 5, end: 9)
        let (_, body2) = try await storage.getObject(
            bucket: "range-bucket", key: "digits", versionId: nil, range: range2)

        var received2 = ""
        if let body2 = body2 {
            for await chunk in body2 {
                received2 += String(buffer: chunk)
            }
        }
        #expect(received2 == "56789")

        // Cleanup
        try? await storage.shutdown()
    }
    @Test("Storage: Multipart Upload Flow")
    func testMultipartUploadFlow() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "multi-bucket", owner: "test-owner")

        // 1. Initiate
        let uploadId = try await storage.createMultipartUpload(
            bucket: "multi-bucket", key: "large-file", metadata: nil, owner: "test-owner")
        #expect(!uploadId.isEmpty)

        // 2. Upload Parts
        let part1Content = "Part 1 Data "
        let part1Data = ByteBuffer(string: part1Content)
        let etag1 = try await storage.uploadPart(
            bucket: "multi-bucket", key: "large-file", uploadId: uploadId, partNumber: 1,
            data: [part1Data].async, size: Int64(part1Data.readableBytes))

        let part2Content = "Part 2 Data"
        let part2Data = ByteBuffer(string: part2Content)
        let etag2 = try await storage.uploadPart(
            bucket: "multi-bucket", key: "large-file", uploadId: uploadId, partNumber: 2,
            data: [part2Data].async, size: Int64(part2Data.readableBytes))

        // 3. Complete
        let parts = [
            PartInfo(partNumber: 1, eTag: etag1),
            PartInfo(partNumber: 2, eTag: etag2),
        ]
        let finalETag = try await storage.completeMultipartUpload(
            bucket: "multi-bucket", key: "large-file", uploadId: uploadId, parts: parts)
        #expect(!finalETag.isEmpty)
        #expect(finalETag.contains("-2"))  // Our implementation appends count

        // 4. Verify Content
        let (_, body) = try await storage.getObject(
            bucket: "multi-bucket", key: "large-file", versionId: nil, range: nil)

        var receivedData = ""
        if let body = body {
            for await chunk in body {
                receivedData += String(buffer: chunk)
            }
        }
        #expect(receivedData == part1Content + part2Content)
    }

    @Test("API: Head Bucket")
    func testHeadBucket() async throws {
        try await withApp { app in
            // 1. Create Bucket
            let helper = AWSAuthHelper()
            let bucketUrl = URL(string: "http://localhost:8080/head-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/head-bucket", method: .put, headers: createHeaders) {
                response in
                #expect(response.status == .ok)
            }

            // 2. Head Bucket - Exists
            try await app.execute(uri: "/head-bucket", method: .head) { response in
                #expect(response.status == .ok)
            }

            // 3. Head Bucket - Not Found
            try await app.execute(uri: "/non-existent-bucket", method: .head) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    @Test("API: Delete Bucket (Non-Empty)")
    func testDeleteNonEmptyBucket() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()

            // 1. Create Bucket
            let bucketUrl = URL(string: "http://localhost:8080/delete-check-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(
                uri: "/delete-check-bucket", method: .put, headers: createHeaders
            ) { response in
                #expect(response.status == .ok)
            }

            // 2. Put Object
            let objectUrl = URL(
                string: "http://localhost:8080/delete-check-bucket/obj")!
            let content = "data"
            let putHeaders = try helper.signRequest(
                method: .put, url: objectUrl, payload: content)
            try await app.execute(
                uri: "/delete-check-bucket/obj", method: .put, headers: putHeaders,
                body: ByteBuffer(string: content)
            ) { response in
                #expect(response.status == .ok)
            }

            // 3. Try Delete Bucket - Should Fail (Conflict or Forbidden or internal error mapped)
            // S3 spec says 409 Conflict for BucketNotEmpty.
            // Our current implementation throws S3Error.bucketNotEmpty.
            // We need to check mapping in S3ErrorMiddleware or catch it.
            // S3ErrorMiddleware maps bucketNotEmpty to conflict (409).
            let deleteHeaders = try helper.signRequest(method: .delete, url: bucketUrl)
            try await app.execute(
                uri: "/delete-check-bucket", method: .delete, headers: deleteHeaders
            ) { response in
                #expect(response.status == .conflict)
            }

            // 4. Delete Object
            let deleteObjHeaders = try helper.signRequest(method: .delete, url: objectUrl)
            try await app.execute(
                uri: "/delete-check-bucket/obj", method: .delete, headers: deleteObjHeaders
            ) {
                response in
                #expect(response.status == .noContent)
            }

            // 5. Delete Bucket - Should Succeed
            try await app.execute(
                uri: "/delete-check-bucket", method: .delete, headers: deleteHeaders
            ) { response in
                #expect(response.status == .noContent)
            }
        }
    }

    // MARK: - Checksum Verification Tests

    @Test("Storage: Checksum Verification Passes")
    func testChecksumVerificationPasses() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root, testMode: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "checksum-bucket", owner: "test-owner")

        let content = "Hello Checksum"
        let data = ByteBuffer(string: content)
        let meta = try await storage.putObject(
            bucket: "checksum-bucket", key: "verified.txt", data: [data].async,
            size: Int64(data.readableBytes), metadata: nil, owner: "test-owner")
        let etag = meta.eTag ?? ""

        // The etag IS the SHA256 hex hash — verify it's a valid 64-char hex string
        #expect(etag.count == 64)
        #expect(etag.allSatisfy { $0.isHexDigit })
    }

    @Test("API: Checksum x-amz-content-sha256 Mismatch Rejected")
    func testChecksumMismatchRejected() async throws {
        try await withApp { app in
            // Create Bucket (no auth needed for router-level test)
            try await app.execute(uri: "/checksum-bucket", method: .put) { _ in }

            // PUT with WRONG x-amz-content-sha256 header
            var headers = HTTPFields()
            headers[HTTPField.Name("x-amz-content-sha256")!] =
                "0000000000000000000000000000000000000000000000000000000000000000"
            headers[.contentType] = "text/plain"

            try await app.execute(
                uri: "/checksum-bucket/bad-hash.txt", method: .put, headers: headers,
                body: ByteBuffer(string: "actual content")
            ) { response in
                #expect(response.status == .badRequest)
                let body = String(buffer: response.body)
                #expect(body.contains("XAmzContentSHA256Mismatch"))
            }
        }
    }

    @Test("API: Checksum UNSIGNED-PAYLOAD Skipped")
    func testChecksumUnsignedPayloadSkipped() async throws {
        try await withApp { app in
            // Create Bucket
            try await app.execute(uri: "/unsigned-bucket", method: .put) { _ in }

            // PUT with UNSIGNED-PAYLOAD header — should succeed
            var headers = HTTPFields()
            headers[HTTPField.Name("x-amz-content-sha256")!] = "UNSIGNED-PAYLOAD"
            headers[.contentType] = "text/plain"

            try await app.execute(
                uri: "/unsigned-bucket/file.txt", method: .put, headers: headers,
                body: ByteBuffer(string: "unsigned content")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.eTag] != nil)
            }
        }
    }

    // MARK: - XML Parsing Tests

    @Test("XML: Parse Tagging")
    func testParseTagging() {
        let xml = """
        <Tagging>
            <TagSet>
                <Tag>
                    <Key>Environment</Key>
                    <Value>Production</Value>
                </Tag>
                <Tag>
                    <Key>Owner</Key>
                    <Value>DevOps</Value>
                </Tag>
            </TagSet>
        </Tagging>
        """
        let tags = XML.parseTagging(xml: xml)
        #expect(tags.count == 2)
        #expect(tags[0].key == "Environment")
        #expect(tags[0].value == "Production")
        #expect(tags[1].key == "Owner")
        #expect(tags[1].value == "DevOps")
    }

    @Test("XML: Parse Tagging - Empty")
    func testParseTaggingEmpty() {
        let xml = "<Tagging><TagSet></TagSet></Tagging>"
        let tags = XML.parseTagging(xml: xml)
        #expect(tags.isEmpty)
    }

    @Test("XML: Parse Tagging - Malformed")
    func testParseTaggingMalformed() {
        let xml = "<Tagging><TagSet><Tag><Key>Test</Key></Tag></TagSet></Tagging>"
        let tags = XML.parseTagging(xml: xml)
        #expect(tags.count == 1)
        #expect(tags[0].key == "Test")
        #expect(tags[0].value == "")  // Missing value
    }

    @Test("XML: Parse Delete Objects")
    func testParseDeleteObjects() {
        let xml = """
        <Delete>
            <Object>
                <Key>file1.txt</Key>
            </Object>
            <Object>
                <Key>file2.txt</Key>
            </Object>
        </Delete>
        """
        let objects = XML.parseDeleteObjects(xml: xml)
        #expect(objects == [DeleteObject(key: "file1.txt", versionId: nil), DeleteObject(key: "file2.txt", versionId: nil)])
    }

    @Test("XML: Parse Delete Objects - Empty")
    func testParseDeleteObjectsEmpty() {
        let xml = "<Delete></Delete>"
        let objects = XML.parseDeleteObjects(xml: xml)
        #expect(objects.isEmpty)
    }

    @Test("XML: Parse Delete Objects - With Versions")
    func testParseDeleteObjectsWithVersions() {
        let xml = """
        <Delete>
            <Object>
                <Key>file1.txt</Key>
                <VersionId>v1.0</VersionId>
            </Object>
            <Object>
                <Key>file2.txt</Key>
            </Object>
            <Object>
                <Key>file3.txt</Key>
                <VersionId>v2.1</VersionId>
            </Object>
        </Delete>
        """
        let objects = XML.parseDeleteObjects(xml: xml)
        #expect(objects == [
            DeleteObject(key: "file1.txt", versionId: "v1.0"),
            DeleteObject(key: "file2.txt", versionId: nil),
            DeleteObject(key: "file3.txt", versionId: "v2.1")
        ])
    }

    @Test("XML: Parse Complete Multipart Upload")
    func testParseCompleteMultipartUpload() {
        let xml = """
        <CompleteMultipartUpload>
            <Part>
                <ETag>"etag1"</ETag>
                <PartNumber>1</PartNumber>
            </Part>
            <Part>
                <ETag>"etag2"</ETag>
                <PartNumber>2</PartNumber>
            </Part>
        </CompleteMultipartUpload>
        """
        let parts = XML.parseCompleteMultipartUpload(xml: xml)
        #expect(parts.count == 2)
        #expect(parts[0].partNumber == 1)
        #expect(parts[0].eTag == "etag1")
        #expect(parts[1].partNumber == 2)
        #expect(parts[1].eTag == "etag2")
    }

    // MARK: - Error Handling Tests

    @Test("API: Malformed XML in Delete Objects")
    func testMalformedXMLDeleteObjects() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()
            let bucketUrl = URL(string: "http://localhost:8080/error-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/error-bucket", method: .put, headers: createHeaders) { _ in }

            let malformedXML = "<Delete><Object><Key>file1.txt</Key></Object><Invalid></Delete>"
            let deleteUrl = URL(string: "http://localhost:8080/error-bucket?delete")!
            let deleteHeaders = try helper.signRequest(method: .post, url: deleteUrl, payload: malformedXML)

            try await app.execute(uri: "/error-bucket?delete", method: .post, headers: deleteHeaders,
                                body: ByteBuffer(string: malformedXML)) { response in
                // Should still work as parsing is lenient
                #expect(response.status == .ok)
            }
        }
    }

    @Test("API: Invalid Bucket Policy JSON")
    func testInvalidBucketPolicy() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()
            let bucketUrl = URL(string: "http://localhost:8080/policy-error-bucket")!
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/policy-error-bucket", method: .put, headers: createHeaders) { _ in }

            let invalidPolicy = "{ invalid json }"
            let policyUrl = URL(string: "http://localhost:8080/policy-error-bucket?policy")!
            let policyHeaders = try helper.signRequest(method: .put, url: policyUrl, payload: invalidPolicy)

            try await app.execute(uri: "/policy-error-bucket?policy", method: .put, headers: policyHeaders,
                                body: ByteBuffer(string: invalidPolicy)) { response in
                // Should handle JSON parsing errors gracefully with 400 Bad Request
                #expect(response.status == .badRequest)
            }
        }
    }

    @Test("API: Non-existent Object Access")
    func testNonExistentObject() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()
            let objectUrl = URL(string: "http://localhost:8080/test-bucket/nonexistent.txt")!
            let headers = try helper.signRequest(method: .get, url: objectUrl)

            try await app.execute(uri: "/test-bucket/nonexistent.txt", method: .get, headers: headers) { response in
                #expect(response.status == .notFound)
            }
        }
    }

    // MARK: - Property-Based Testing (Fuzzing)

    @Test("XML: Parse Tagging - Fuzzing")
    func testParseTaggingFuzzing() {
        // Test various edge cases and malformed inputs
        let testCases = [
            "<Tagging><TagSet></TagSet></Tagging>",
            "<Tagging><TagSet><Tag><Key></Key><Value></Value></Tag></TagSet></Tagging>",
            "<Tagging><TagSet><Tag><Key>Key1</Key><Value>Val1</Value></Tag><Tag><Key>Key2</Key><Value>Val2</Value></Tag></TagSet></Tagging>",
            "<Tagging><TagSet><Tag><Key>Special_Chars-123</Key><Value>!@#$%^&*()</Value></Tag></TagSet></Tagging>",
            "<Tagging><TagSet><Tag><Key>Unicode_🚀</Key><Value>测试</Value></Tag></TagSet></Tagging>",
            "<Tagging><TagSet><Tag><Key>LongKey\(String(repeating: "A", count: 100))</Key><Value>LongValue\(String(repeating: "B", count: 200))</Value></Tag></TagSet></Tagging>",
        ]

        for xml in testCases {
            // Should not crash
            let tags = XML.parseTagging(xml: xml)
            #expect(tags.count >= 0)  // Should return valid array
            for tag in tags {
                #expect(!tag.key.isEmpty)  // Keys should not be empty in valid tags
            }
        }
    }

    @Test("XML: Parse Delete Objects - Fuzzing")
    func testParseDeleteObjectsFuzzing() {
        let testCases = [
            "<Delete></Delete>",
            "<Delete><Object><Key></Key></Object></Delete>",
            "<Delete><Object><Key>file1.txt</Key></Object><Object><Key>file2.txt</Key></Object></Delete>",
            "<Delete><Object><Key>special_file-123_🚀.txt</Key></Object></Delete>",
            "<Delete><Object><Key>\(String(repeating: "A", count: 1000))</Key></Object></Delete>",
        ]

        for xml in testCases {
            // Should not crash - fuzzing tests edge cases and malformed input
            let objects = XML.parseDeleteObjects(xml: xml)
            #expect(objects.count >= 0)  // Should return a valid array
            // Note: Empty keys are allowed in malformed input, we just test for no crashes
        }
    }

    @Test("XML: Parse Multipart Upload - Fuzzing")
    func testParseMultipartUploadFuzzing() {
        let testCases = [
            "<CompleteMultipartUpload></CompleteMultipartUpload>",
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag1</ETag></Part></CompleteMultipartUpload>",
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"etag1\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>etag2</ETag></Part></CompleteMultipartUpload>",
            "<CompleteMultipartUpload><Part><PartNumber>0</PartNumber><ETag>invalid</ETag></Part></CompleteMultipartUpload>", // Invalid part number
            "<CompleteMultipartUpload><Part><PartNumber>10000</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>", // Large part number
        ]

        for xml in testCases {
            // Should not crash
            let parts = XML.parseCompleteMultipartUpload(xml: xml)
            #expect(parts.count >= 0)
            for part in parts {
                #expect(part.partNumber > 0)  // Valid parts should have positive numbers
                #expect(!part.eTag.isEmpty)   // Valid parts should have etags
            }
        }
    }
}
