import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import Testing

@testable import SwiftS3

@Suite("SwiftS3 Tests")
struct SwiftS3Tests {

    // MARK: - Helper
    func withApp(_ test: @escaping @Sendable (any TestClientProtocol) async throws -> Void)
        async throws
    {
        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        let server = S3Server(
            hostname: "127.0.0.1", port: 0, storagePath: storagePath, accessKey: "admin",
            secretKey: "password")

        let storage = FileSystemStorage(rootPath: storagePath)
        let controller = S3Controller(storage: storage)

        let router = Router()
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(accessKey: "admin", secretKey: "password"))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(server.hostname, port: server.port))
        )

        try await app.test(.router, test)

        // Cleanup
        try? FileManager.default.removeItem(atPath: storagePath)
    }

    // MARK: - Storage Tests

    @Test("Storage: ListObjects Pagination & Filtering")
    func testStorageListObjectsPagination() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "list-bucket")

        // Create nested structure
        // a/1.txt
        // a/2.txt
        // b/1.txt
        // c.txt
        let data = ByteBuffer(string: ".")
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "a/1.txt", data: [data].async, size: 1, metadata: nil)
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "a/2.txt", data: [data].async, size: 1, metadata: nil)
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "b/1.txt", data: [data].async, size: 1, metadata: nil)
        _ = try await storage.putObject(
            bucket: "list-bucket", key: "c.txt", data: [data].async, size: 1, metadata: nil)

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
    }

    @Test("Storage: Copy Object")
    func testStorageCopyObject() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        )
        .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "src-bucket")
        try await storage.createBucket(name: "dst-bucket")

        let data = ByteBuffer(string: "Copy Me")
        let metadata = ["x-amz-meta-original": "true"]
        _ = try await storage.putObject(
            bucket: "src-bucket", key: "source.txt", data: [data].async,
            size: Int64(data.readableBytes), metadata: metadata)

        // Copy
        let copyMeta = try await storage.copyObject(
            fromBucket: "src-bucket", fromKey: "source.txt", toBucket: "dst-bucket",
            toKey: "copied.txt")

        #expect(copyMeta.key == "copied.txt")
        #expect(copyMeta.size == Int64(data.readableBytes))
        #expect(copyMeta.customMetadata["x-amz-meta-original"] == "true")

        // Verify content
        let (_, body) = try await storage.getObject(
            bucket: "dst-bucket", key: "copied.txt", range: nil)
        var received = ""
        if let body = body {
            for await chunk in body {
                received += String(buffer: chunk)
            }
        }
        #expect(received == "Copy Me")
    }

    @Test("Storage: Create and Delete Bucket")
    func testStorageCreateDeleteBucket() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "test-bucket")
        let result = try await storage.listObjects(
            bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil,
            maxKeys: nil)
        #expect(result.objects.count == 0)

        try await storage.deleteBucket(name: "test-bucket")
        let bucketsAfter = try await storage.listBuckets()
        #expect(bucketsAfter.isEmpty)
    }

    @Test("Storage: Put and Get Object")
    func testStoragePutGetObject() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "test-bucket")

        let data = Data("Hello, World!".utf8)
        let buffer = ByteBuffer(data: data)
        let stream = AsyncStream<ByteBuffer> { continuation in
            continuation.yield(buffer)
            continuation.finish()
        }

        let etag = try await storage.putObject(
            bucket: "test-bucket", key: "hello.txt", data: stream, size: Int64(data.count),
            metadata: nil)
        #expect(!etag.isEmpty)

        let result = try await storage.listObjects(
            bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil,
            maxKeys: nil)
        #expect(result.objects.count == 1)
        #expect(result.objects.first?.key == "hello.txt")

        let (metadata, body) = try await storage.getObject(
            bucket: "test-bucket", key: "hello.txt", range: nil)
        #expect(metadata.size == Int64(data.count))

        var receivedData = Data()
        if let body = body {
            for await chunk in body {
                receivedData.append(contentsOf: chunk.readableBytesView)
            }
        }
        #expect(receivedData == data)
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
        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        defer { try? FileManager.default.removeItem(atPath: storagePath) }

        let customAccess = "user123"
        let customSecret = "secret456"

        let router = Router()
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(accessKey: customAccess, secretKey: customSecret))
        let storage = FileSystemStorage(rootPath: storagePath)
        S3Controller(storage: storage).addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname("127.0.0.1", port: 0))
        )

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
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "meta-bucket")

        let data = ByteBuffer(string: "Hello Metadata")
        let metadata = ["x-amz-meta-custom": "value123", "Content-Type": "application/json"]

        _ = try await storage.putObject(
            bucket: "meta-bucket", key: "obj", data: [data].async, size: Int64(data.readableBytes),
            metadata: metadata)

        let (readMeta, _) = try await storage.getObject(
            bucket: "meta-bucket", key: "obj", range: nil)

        #expect(readMeta.contentType == "application/json")
        #expect(readMeta.customMetadata["x-amz-meta-custom"] == "value123")

        // head object
        let headMeta = try await storage.getObjectMetadata(bucket: "meta-bucket", key: "obj")
        #expect(headMeta.contentType == "application/json")
        #expect(headMeta.customMetadata["x-amz-meta-custom"] == "value123")
    }

    @Test("Storage: Range Requests")
    func testStorageRange() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "range-bucket")

        let content = "0123456789"
        let data = ByteBuffer(string: content)
        _ = try await storage.putObject(
            bucket: "range-bucket", key: "digits", data: [data].async,
            size: Int64(data.readableBytes), metadata: nil)

        // Test Range: 0-4 (5 bytes) -> "01234"
        let range1 = ValidatedRange(start: 0, end: 4)
        let (_, body1) = try await storage.getObject(
            bucket: "range-bucket", key: "digits", range: range1)

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
            bucket: "range-bucket", key: "digits", range: range2)

        var received2 = ""
        if let body2 = body2 {
            for await chunk in body2 {
                received2 += String(buffer: chunk)
            }
        }
        #expect(received2 == "56789")
    }
    @Test("Storage: Multipart Upload Flow")
    func testMultipartUploadFlow() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "multi-bucket")

        // 1. Initiate
        let uploadId = try await storage.createMultipartUpload(
            bucket: "multi-bucket", key: "large-file", metadata: nil)
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
            bucket: "multi-bucket", key: "large-file", range: nil)

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
}
