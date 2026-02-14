import NIO
import SQLiteNIO
import XCTest

@testable import SwiftS3

final class SQLStorageTests: XCTestCase {

    // Shared resources
    static let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    static let threadPool: NIOThreadPool = {
        let tp = NIOThreadPool(numberOfThreads: 2)
        tp.start()
        return tp
    }()

    override class func tearDown() {
        // We can't easily shutdown static resources in XCTest on Linux reliably in all versions,
        // but we can try. Or just let them leak as it's the end of test run.
        try? threadPool.syncShutdownGracefully()  // This might block/crash if XCTest has issues, but worth a try or skip.
        try? elg.syncShutdownGracefully()
    }

    // MARK: - Helper
    func withSQLStorage(_ test: @escaping (FileSystemStorage) async throws -> Void) async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        try? FileManager.default.createDirectory(atPath: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        var metadataStore: SQLMetadataStore?
        do {
            metadataStore = try await SQLMetadataStore.create(
                path: root + "/metadata.sqlite",
                on: SQLStorageTests.elg,
                threadPool: SQLStorageTests.threadPool
            )

            if let store = metadataStore {
                let storage = FileSystemStorage(rootPath: root, metadataStore: store, testMode: true)
                try await test(storage)
                try await storage.shutdown()
                try await store.shutdown()
            }
        } catch {
            try await metadataStore?.shutdown()
            throw error
        }
    }

    // MARK: - Tests

    func testStorageListObjectsPagination() async throws {
        try await withSQLStorage { storage in
            try await storage.createBucket(name: "list-bucket", owner: "test-owner")

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
            XCTAssertEqual(res1.objects.count, 2)
            if res1.objects.count > 0 {
                XCTAssertEqual(res1.objects[0].key, "a/1.txt")
            }

            // 2. Delimiter
            let res2 = try await storage.listObjects(
                bucket: "list-bucket", prefix: nil, delimiter: "/", marker: nil,
                continuationToken: nil, maxKeys: 1000)
            XCTAssertEqual(res2.objects.count, 1)
            if res2.objects.count > 0 {
                XCTAssertEqual(res2.objects[0].key, "c.txt")
            }
            XCTAssertEqual(res2.commonPrefixes.count, 2)
        }
    }

    func testStorageCopyObject() async throws {
        try await withSQLStorage { storage in
            try await storage.createBucket(name: "src-bucket", owner: "test-owner")
            try await storage.createBucket(name: "dst-bucket", owner: "test-owner")

            let data = ByteBuffer(string: "Copy Me")
            let metadata = ["x-amz-meta-original": "true"]
            _ = try await storage.putObject(
                bucket: "src-bucket", key: "source.txt", data: [data].async,
                size: Int64(data.readableBytes), metadata: metadata, owner: "test-owner")

            let copyMeta = try await storage.copyObject(
                fromBucket: "src-bucket", fromKey: "source.txt", toBucket: "dst-bucket",
                toKey: "copied.txt", owner: "test-owner")

            XCTAssertEqual(copyMeta.key, "copied.txt")
            XCTAssertEqual(copyMeta.customMetadata["x-amz-meta-original"], "true")
        }
    }

    func testStorageCreateDeleteBucket() async throws {
        try await withSQLStorage { storage in
            try await storage.createBucket(name: "test-bucket", owner: "test-owner")
            let result = try await storage.listObjects(
                bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil,
                continuationToken: nil, maxKeys: nil)
            XCTAssertEqual(result.objects.count, 0)

            try await storage.deleteBucket(name: "test-bucket")
            let bucketsAfter = try await storage.listBuckets()
            XCTAssertTrue(bucketsAfter.isEmpty)
        }
    }

    func testStoragePutGetObject() async throws {
        try await withSQLStorage { storage in
            try await storage.createBucket(name: "test-bucket", owner: "test-owner")
            let data = Data("Hello, SQL!".utf8)
            let buffer = ByteBuffer(bytes: data)
            _ = try await storage.putObject(
                bucket: "test-bucket", key: "hello.txt", data: [buffer].async,
                size: Int64(data.count), metadata: nil, owner: "test-owner")

            let (metadata, body) = try await storage.getObject(
                bucket: "test-bucket", key: "hello.txt", versionId: nil, range: nil)
            XCTAssertEqual(metadata.size, Int64(data.count))

            var receivedData = Data()
            if let body = body {
                for await chunk in body {
                    receivedData.append(contentsOf: chunk.readableBytesView)
                }
            }
            XCTAssertEqual(receivedData, data)
        }
    }

    func testStorageMetadata() async throws {
        try await withSQLStorage { storage in
            try await storage.createBucket(name: "meta-bucket", owner: "test-owner")
            let data = ByteBuffer(string: "Hello Metadata")
            let metadata = ["x-amz-meta-custom": "value123", "Content-Type": "application/json"]

            _ = try await storage.putObject(
                bucket: "meta-bucket", key: "obj", data: [data].async,
                size: Int64(data.readableBytes), metadata: metadata, owner: "test-owner")

            let (readMeta, _) = try await storage.getObject(
                bucket: "meta-bucket", key: "obj", versionId: nil, range: nil)
            XCTAssertEqual(readMeta.contentType, "application/json")
            XCTAssertEqual(readMeta.customMetadata["x-amz-meta-custom"], "value123")
        }
    }

    func testStorageTagging() async throws {
        try await withSQLStorage { storage in
            let bucket = "tag-bucket"
            try await storage.createBucket(name: bucket, owner: "test-owner")

            // 1. Bucket Tagging
            let bTags = [S3Tag(key: "B1", value: "V1"), S3Tag(key: "B2", value: "V2")]
            try await storage.putTags(bucket: bucket, key: nil, versionId: nil, tags: bTags)

            let retrievedBTags = try await storage.getTags(bucket: bucket, key: nil, versionId: nil)
            XCTAssertEqual(retrievedBTags.count, 2)
            XCTAssertTrue(retrievedBTags.contains(where: { $0.key == "B1" && $0.value == "V1" }))

            try await storage.deleteTags(bucket: bucket, key: nil, versionId: nil)
            let bTagsAfter = try await storage.getTags(bucket: bucket, key: nil, versionId: nil)
            XCTAssertTrue(bTagsAfter.isEmpty)

            // 2. Object Tagging
            let data = ByteBuffer(string: "Tag Me")
            _ = try await storage.putObject(
                bucket: bucket, key: "t1", data: [data].async, size: 6, metadata: nil,
                owner: "test-owner")

            let oTags = [S3Tag(key: "O1", value: "V1")]
            try await storage.putTags(bucket: bucket, key: "t1", versionId: nil, tags: oTags)

            let retrievedOTags = try await storage.getTags(
                bucket: bucket, key: "t1", versionId: nil)
            XCTAssertEqual(retrievedOTags.count, 1)
            XCTAssertEqual(retrievedOTags[0].key, "O1")

            try await storage.deleteTags(bucket: bucket, key: "t1", versionId: nil)
            let oTagsAfter = try await storage.getTags(bucket: bucket, key: "t1", versionId: nil)
            XCTAssertTrue(oTagsAfter.isEmpty)
        }
    }
}
