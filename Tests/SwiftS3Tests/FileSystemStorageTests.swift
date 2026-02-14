import Foundation
import NIO
import _NIOFileSystem
import XCTest

@testable import SwiftS3

final class FileSystemStorageTests: XCTestCase {

    var storage: FileSystemStorage!
    var tempDir: String!

    override func setUp() async throws {
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        storage = FileSystemStorage(rootPath: tempDir)
    }

    override func tearDown() async throws {
        try? await storage.shutdown()
        try? FileManager.default.removeItem(atPath: tempDir)
    }

    func testCreateAndListBuckets() async throws {
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")
        let buckets = try await storage.listBuckets()
        XCTAssertEqual(buckets.count, 1)
        XCTAssertEqual(buckets[0].name, "test-bucket")
    }

    func testDeleteBucket() async throws {
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")
        try await storage.deleteBucket(name: "test-bucket")
        let buckets = try await storage.listBuckets()
        XCTAssert(buckets.isEmpty)
    }

    func testPutAndGetObject() async throws {
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello World")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")

        let retrieved = try await storage.getObject(bucket: "test-bucket", key: "test-key", versionId: nil, range: nil)
        XCTAssertEqual(retrieved.metadata.key, "test-key")
        XCTAssertNotNil(retrieved.body)
    }

    func testDeleteObject() async throws {
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello World")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")

        _ = try await storage.deleteObject(bucket: "test-bucket", key: "test-key", versionId: nil)
        do {
            _ = try await storage.getObject(bucket: "test-bucket", key: "test-key", versionId: nil, range: nil)
            XCTFail("Should have thrown")
        } catch {
            XCTAssert(error is S3Error)
        }
    }

    func testListObjects() async throws {
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello World")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key1", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key2", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")

        let result = try await storage.listObjects(bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil, maxKeys: 1000)
        XCTAssertEqual(result.objects.count, 2)
    }
}