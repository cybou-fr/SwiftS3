import Foundation
import NIO
import Testing

@testable import SwiftS3

@Suite("Object Expiration Tests")
struct ObjectExpirationTests {

    @Test("LifecycleJanitor: Deletes expired objects")
    func testJanitorDeletesExpired() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        do {
            let metadataStore = try await SQLMetadataStore.create(
                path: storagePath + "/metadata.sqlite",
                on: elg,
                threadPool: threadPool
            )
            let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)

            let bucket = "test-bucket"
            try await storage.createBucket(name: bucket, owner: "admin")

            // 1. Create an object
            let key = "old-object.txt"
            _ = try await storage.putObject(
                bucket: bucket, key: key, data: [ByteBuffer(string: "hello")].async,
                size: 5, metadata: nil, owner: "admin"
            )

            // 2. Backdate the object to 40 days ago
            let oldDate = Date().addingTimeInterval(-60 * 60 * 24 * 40)
            var metadata = try await metadataStore.getMetadata(
                bucket: bucket, key: key, versionId: nil)
            metadata.lastModified = oldDate
            try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: metadata)

            // 3. Add a lifecycle rule: expire in 30 days
            let lifecycle = LifecycleConfiguration(rules: [
                .init(
                    id: "rule1",
                    status: .enabled,
                    filter: .init(prefix: ""),
                    expiration: .init(days: 30),
                    noncurrentVersionExpiration: nil
                )
            ])
            try await storage.putBucketLifecycle(bucket: bucket, configuration: lifecycle)

            // 4. Run Janitor
            let janitor = LifecycleJanitor(storage: storage)
            try await janitor.performExpiration()

            // 5. Verify object is deleted
            await #expect(throws: S3Error.noSuchKey) {
                try await storage.getObjectMetadata(bucket: bucket, key: key, versionId: nil)
            }

            try await metadataStore.shutdown()
        } catch {
            throw error
        }

        try await threadPool.shutdownGracefully()
        try await elg.shutdownGracefully()
        try? FileManager.default.removeItem(atPath: storagePath)
    }

    @Test("LifecycleJanitor: Respects prefix filter")
    func testJanitorRespectsPrefix() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        do {
            let metadataStore = try await SQLMetadataStore.create(
                path: storagePath + "/metadata.sqlite",
                on: elg,
                threadPool: threadPool
            )
            let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)

            let bucket = "prefix-bucket"
            try await storage.createBucket(name: bucket, owner: "admin")

            // 1. Create two old objects, one matching prefix, one not
            let oldDate = Date().addingTimeInterval(-60 * 60 * 24 * 40)

            let key1 = "logs/old.log"
            _ = try await storage.putObject(
                bucket: bucket, key: key1, data: [ByteBuffer(string: "data")].async, size: 4,
                metadata: nil, owner: "admin")
            var meta1 = try await metadataStore.getMetadata(
                bucket: bucket, key: key1, versionId: nil)
            meta1.lastModified = oldDate
            try await metadataStore.saveMetadata(bucket: bucket, key: key1, metadata: meta1)

            let key2 = "important/old.txt"
            _ = try await storage.putObject(
                bucket: bucket, key: key2, data: [ByteBuffer(string: "data")].async, size: 4,
                metadata: nil, owner: "admin")
            var meta2 = try await metadataStore.getMetadata(
                bucket: bucket, key: key2, versionId: nil)
            meta2.lastModified = oldDate
            try await metadataStore.saveMetadata(bucket: bucket, key: key2, metadata: meta2)

            // 2. Add rule with prefix "logs/"
            let lifecycle = LifecycleConfiguration(rules: [
                .init(
                    id: "rule-prefix",
                    status: .enabled,
                    filter: .init(prefix: "logs/"),
                    expiration: .init(days: 30),
                    noncurrentVersionExpiration: nil
                )
            ])
            try await storage.putBucketLifecycle(bucket: bucket, configuration: lifecycle)

            // 3. Run Janitor
            let janitor = LifecycleJanitor(storage: storage)
            try await janitor.performExpiration()

            // 4. Verify: logs/old.log deleted, important/old.txt remains
            await #expect(throws: S3Error.noSuchKey) {
                try await storage.getObjectMetadata(bucket: bucket, key: key1, versionId: nil)
            }

            let remaining = try await storage.getObjectMetadata(
                bucket: bucket, key: key2, versionId: nil)
            #expect(remaining.key == key2)

            try await metadataStore.shutdown()
        } catch {
            throw error
        }

        try await threadPool.shutdownGracefully()
        try await elg.shutdownGracefully()
        try? FileManager.default.removeItem(atPath: storagePath)
    }

    @Test("LifecycleJanitor: Deletes noncurrent versions after days")
    func testJanitorDeletesNoncurrentVersions() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        do {
            let metadataStore = try await SQLMetadataStore.create(
                path: storagePath + "/metadata.sqlite",
                on: elg,
                threadPool: threadPool
            )
            let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)

            let bucket = "versioned-bucket"
            try await storage.createBucket(name: bucket, owner: "admin")
            try await storage.putBucketVersioning(bucket: bucket, configuration: VersioningConfiguration(status: .enabled))

            // Create multiple versions of the same object
            let key = "test-object.txt"

            // Version 1 (oldest)
            _ = try await storage.putObject(
                bucket: bucket, key: key, data: [ByteBuffer(string: "version 1")].async, size: 9,
                metadata: nil, owner: "admin")
            let oldDate = Date().addingTimeInterval(-60 * 60 * 24 * 40) // 40 days ago
            var meta1 = try await metadataStore.getMetadata(bucket: bucket, key: key, versionId: nil)
            meta1.lastModified = oldDate
            try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: meta1)

            // Version 2 (current, recent)
            _ = try await storage.putObject(
                bucket: bucket, key: key, data: [ByteBuffer(string: "version 2")].async, size: 9,
                metadata: nil, owner: "admin")

            // Add lifecycle rule: expire noncurrent versions after 30 days
            let lifecycle = LifecycleConfiguration(rules: [
                .init(
                    id: "noncurrent-rule",
                    status: .enabled,
                    filter: .init(prefix: ""),
                    expiration: nil,
                    noncurrentVersionExpiration: .init(noncurrentDays: 30, newerNoncurrentVersions: nil)
                )
            ])
            try await storage.putBucketLifecycle(bucket: bucket, configuration: lifecycle)

            // Run Janitor
            let janitor = LifecycleJanitor(storage: storage)
            try await janitor.performExpiration()

            // Verify: noncurrent version (v1) is deleted, current version (v2) remains
            let versions = try await storage.listObjectVersions(bucket: bucket, prefix: nil, delimiter: nil, keyMarker: nil, versionIdMarker: nil, maxKeys: 100)
            #expect(versions.versions.count == 1)
            #expect(versions.versions[0].key == key)
            #expect(versions.versions[0].versionId != meta1.versionId) // Not the old version

            try await metadataStore.shutdown()
        } catch {
            throw error
        }

        try await threadPool.shutdownGracefully()
        try await elg.shutdownGracefully()
        try? FileManager.default.removeItem(atPath: storagePath)
    }
}

extension Array where Element: Sendable {
    var async: AsyncStream<Element> {
        AsyncStream { continuation in
            for element in self {
                continuation.yield(element)
            }
            continuation.finish()
        }
    }
}
