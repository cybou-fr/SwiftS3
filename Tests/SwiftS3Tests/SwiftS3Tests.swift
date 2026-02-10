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
        let server = S3Server(hostname: "127.0.0.1", port: 0, storagePath: storagePath)

        let storage = FileSystemStorage(rootPath: storagePath)
        let controller = S3Controller(storage: storage)

        let router = Router()
        router.middlewares.add(S3Authenticator())
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

    @Test("Storage: Create and Delete Bucket")
    func testStorageCreateDeleteBucket() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        let storage = FileSystemStorage(rootPath: root)
        defer { try? FileManager.default.removeItem(atPath: root) }

        try await storage.createBucket(name: "test-bucket")
        let buckets = try await storage.listBuckets()
        #expect(buckets.count == 1)
        #expect(buckets.first?.name == "test-bucket")

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

    @Test("API: Create Bucket (Auth Required - Fail)")
    func testAPICreateBucketAuthFail() async throws {
        try await withApp { app in
            // Authenticator allows anonymous if no header is present,
            // BUT S3 usually requires auth for writes.
            // Our current S3Authenticator implementation allows everything if no header provided
            // UNLESS the operation specifically checks permissions (which it doesn't currently).
            // However, if we provide a BAD header, it should fail.

            try await app.execute(
                uri: "/mybucket", method: HTTPTypes.HTTPRequest.Method.put,
                headers: [HTTPField.Name.authorization: "AWS4-HMAC-SHA256 BADHEADER"]
            ) { response in
                // Currently S3Authenticator throws S3Error.signatureDoesNotMatch if bad header
                // Note: Hummingbird might return 500 for unhandled errors, or we need to map S3Error to Response
                // Let's see what happens.
                #expect(response.status != HTTPResponse.Status.ok)
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
}
