import Foundation
import NIO
import _NIOFileSystem
import XCTest

@testable import SwiftS3

final class FileSystemStorageTests: XCTestCase {

    func testCreateAndListBuckets() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let storage = FileSystemStorage(rootPath: tempDir)
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")
        let buckets = try await storage.listBuckets()
        XCTAssertEqual(buckets.count, 1)
        XCTAssertEqual(buckets[0].name, "test-bucket")
    }

    func testDeleteBucket() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let storage = FileSystemStorage(rootPath: tempDir)
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")
        try await storage.deleteBucket(name: "test-bucket")
        let buckets = try await storage.listBuckets()
        XCTAssert(buckets.isEmpty)
    }

    func testPutAndGetObject() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let storage = FileSystemStorage(rootPath: tempDir)
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello World")
        let metadata = try await storage.putObject(bucket: "test-bucket", key: "test-key", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")

        let retrieved = try await storage.getObject(bucket: "test-bucket", key: "test-key", versionId: nil, range: nil)
        XCTAssertEqual(retrieved.metadata.key, "test-key")
        XCTAssertNotNil(retrieved.body)
    }

    func testDeleteObject() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let storage = FileSystemStorage(rootPath: tempDir)
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello World")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")

        try await storage.deleteObject(bucket: "test-bucket", key: "test-key", versionId: nil)
        do {
            _ = try await storage.getObject(bucket: "test-bucket", key: "test-key", versionId: nil, range: nil)
            XCTFail("Should have thrown")
        } catch {
            XCTAssert(error is S3Error)
        }
    }

    func testListObjects() async throws {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let storage = FileSystemStorage(rootPath: tempDir)
        try await storage.createBucket(name: "test-bucket", owner: "test-owner")

        let data = ByteBuffer(string: "Hello World")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key1", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")
        _ = try await storage.putObject(bucket: "test-bucket", key: "test-key2", data: [data].async, size: Int64(data.readableBytes), metadata: [:], owner: "test-owner")

        let result = try await storage.listObjects(bucket: "test-bucket", prefix: nil, delimiter: nil, marker: nil, continuationToken: nil, maxKeys: 1000)
        XCTAssertEqual(result.objects.count, 2)
    }
}