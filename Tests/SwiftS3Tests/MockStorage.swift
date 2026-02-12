import Foundation
import NIO

@testable import SwiftS3

/// Mock storage backend for testing controller logic in isolation
actor MockStorage: StorageBackend {
    // Storage for mock data
    private var buckets: Set<String> = []
    private var objects: [String: [String: ObjectMetadata]] = [:] // bucket -> key -> metadata
    private var objectData: [String: [String: Data]] = [:] // bucket -> key -> data
    private var acls: [String: [String: AccessControlPolicy]] = [:] // bucket -> key -> acl
    private var bucketAcls: [String: AccessControlPolicy] = [:] // bucket -> acl
    private var policies: [String: BucketPolicy] = [:] // bucket -> policy
    private var tags: [String: [String: [S3Tag]]] = [:] // bucket -> key -> tags
    private var bucketTags: [String: [S3Tag]] = [:] // bucket -> tags
    private var lifecycleConfigs: [String: LifecycleConfiguration] = [:] // bucket -> config
    private var versioningConfigs: [String: VersioningConfiguration] = [:] // bucket -> config

    // Test helpers
    nonisolated(unsafe) var shouldFailOnCreateBucket = false
    nonisolated(unsafe) var shouldFailOnPutObject = false
    nonisolated(unsafe) var shouldFailOnGetObject = false

    func createBucket(name: String, owner: String) async throws {
        if shouldFailOnCreateBucket {
            throw S3Error.bucketAlreadyExists
        }
        buckets.insert(name)
        // Set default ACL
        let defaultAcl = AccessControlPolicy(
            owner: Owner(id: owner),
            accessControlList: [Grant(grantee: Grantee.user(id: owner), permission: .fullControl)]
        )
        bucketAcls[name] = defaultAcl
    }

    func deleteBucket(name: String) async throws {
        guard buckets.contains(name) else {
            throw S3Error.noSuchBucket
        }
        buckets.remove(name)
        objects.removeValue(forKey: name)
        objectData.removeValue(forKey: name)
        acls.removeValue(forKey: name)
        bucketAcls.removeValue(forKey: name)
        policies.removeValue(forKey: name)
        tags.removeValue(forKey: name)
        bucketTags.removeValue(forKey: name)
        lifecycleConfigs.removeValue(forKey: name)
        versioningConfigs.removeValue(forKey: name)
    }

    func headBucket(name: String) async throws {
        guard buckets.contains(name) else {
            throw S3Error.noSuchBucket
        }
    }

    func listBuckets() async throws -> [(name: String, created: Date)] {
        return buckets.map { (name: $0, created: Date()) }
    }

    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: consuming Stream, size: Int64?,
        metadata: [String: String]?, owner: String
    ) async throws -> ObjectMetadata where Stream.Element == ByteBuffer {
        if shouldFailOnPutObject {
            throw S3Error.internalError
        }

        guard buckets.contains(bucket) else {
            throw S3Error.noSuchBucket
        }

        // Collect data
        var collectedData = Data()
        for try await var buffer in data {
            collectedData.append(contentsOf: buffer.readableBytesView)
        }

        let actualSize = Int64(collectedData.count)
        let eTag = "\"\(String(format: "%02x", collectedData.hashValue))\"" // Simple mock ETag

        let objectMetadata = ObjectMetadata(
            key: key,
            size: actualSize,
            lastModified: Date(),
            eTag: eTag,
            contentType: metadata?["Content-Type"],
            customMetadata: metadata?.filter { !$0.key.lowercased().hasPrefix("x-amz-meta-") } ?? [:],
            owner: owner,
            versionId: "null",
            isLatest: true,
            isDeleteMarker: false
        )

        if objects[bucket] == nil {
            objects[bucket] = [:]
            objectData[bucket] = [:]
        }
        objects[bucket]![key] = objectMetadata
        objectData[bucket]![key] = collectedData

        return objectMetadata
    }

    func getObject(bucket: String, key: String, versionId: String?, range: ValidatedRange?) async throws -> (metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?) {
        if shouldFailOnGetObject {
            throw S3Error.noSuchKey
        }

        guard buckets.contains(bucket), let bucketObjects = objects[bucket], let metadata = bucketObjects[key] else {
            throw S3Error.noSuchKey
        }

        guard let data = objectData[bucket]?[key] else {
            throw S3Error.internalError
        }

        let stream = AsyncStream<ByteBuffer> { continuation in
            if let range = range {
                let start = Int(range.start)
                let end = min(Int(range.end), data.count - 1)
                if start <= end {
                    let rangeData = data[start...end]
                    continuation.yield(ByteBuffer(bytes: rangeData))
                }
            } else {
                continuation.yield(ByteBuffer(bytes: data))
            }
            continuation.finish()
        }

        return (metadata, stream)
    }

    func getObjectMetadata(bucket: String, key: String, versionId: String?) async throws -> ObjectMetadata {
        guard buckets.contains(bucket), let bucketObjects = objects[bucket], let metadata = bucketObjects[key] else {
            throw S3Error.noSuchKey
        }
        return metadata
    }

    func deleteObject(bucket: String, key: String, versionId: String?) async throws -> (versionId: String?, isDeleteMarker: Bool) {
        guard buckets.contains(bucket) else {
            throw S3Error.noSuchBucket
        }
        objects[bucket]?.removeValue(forKey: key)
        objectData[bucket]?.removeValue(forKey: key)
        acls[bucket]?.removeValue(forKey: key)
        tags[bucket]?.removeValue(forKey: key)
        return (versionId: "null", isDeleteMarker: false)
    }

    func deleteObjects(bucket: String, keys: [String]) async throws -> [String] {
        var results: [String] = []
        for key in keys {
            do {
                try await deleteObject(bucket: bucket, key: key, versionId: nil)
                results.append(key)
            } catch {
                // Key not found, still include in results
            }
        }
        return results
    }

    func listObjects(bucket: String, prefix: String?, delimiter: String?, marker: String?, continuationToken: String?, maxKeys: Int?) async throws -> ListObjectsResult {
        guard buckets.contains(bucket), let bucketObjects = objects[bucket] else {
            throw S3Error.noSuchBucket
        }

        var filteredObjects = bucketObjects.values.filter { metadata in
            if let prefix = prefix {
                return metadata.key.hasPrefix(prefix)
            }
            return true
        }.sorted { $0.key < $1.key }

        if let marker = marker {
            filteredObjects = filteredObjects.filter { $0.key > marker }
        }

        let limit = maxKeys ?? 1000
        let limitedObjects = Array(filteredObjects.prefix(limit))

        return ListObjectsResult(
            objects: limitedObjects,
            commonPrefixes: [],
            isTruncated: filteredObjects.count > limit,
            nextMarker: limitedObjects.last?.key,
            nextContinuationToken: nil
        )
    }

    func listObjectVersions(bucket: String, prefix: String?, delimiter: String?, keyMarker: String?, versionIdMarker: String?, maxKeys: Int?) async throws -> ListVersionsResult {
        guard buckets.contains(bucket), let bucketObjects = objects[bucket] else {
            throw S3Error.noSuchBucket
        }

        let filteredObjects = bucketObjects.filter { (key, _) in
            if let prefix = prefix {
                return key.hasPrefix(prefix)
            }
            return true
        }.sorted { $0.key < $1.key }

        let limit = maxKeys ?? 1000
        let limitedObjects = Array(filteredObjects.prefix(limit))

        return ListVersionsResult(
            versions: limitedObjects.map { $0.value },
            commonPrefixes: [],
            isTruncated: filteredObjects.count > limit,
            nextKeyMarker: limitedObjects.last?.key,
            nextVersionIdMarker: nil
        )
    }

    func copyObject(fromBucket: String, fromKey: String, toBucket: String, toKey: String, owner: String) async throws -> ObjectMetadata {
        let (sourceMetadata, sourceStream) = try await getObject(bucket: fromBucket, key: fromKey, versionId: nil, range: nil)

        // Collect source data
        var sourceData = Data()
        if let stream = sourceStream {
            for try await buffer in stream {
                sourceData.append(contentsOf: buffer.readableBytesView)
            }
        }

        // Create destination stream
        let destStream = AsyncStream<ByteBuffer> { continuation in
            continuation.yield(ByteBuffer(bytes: sourceData))
            continuation.finish()
        }

        return try await putObject(bucket: toBucket, key: toKey, data: destStream, size: sourceMetadata.size, metadata: nil, owner: owner)
    }

    // ACL methods
    func getACL(bucket: String, key: String?, versionId: String?) async throws -> AccessControlPolicy {
        if let key = key {
            guard let bucketAcls = acls[bucket], let acl = bucketAcls[key] else {
                throw S3Error.noSuchKey
            }
            return acl
        } else {
            guard let acl = bucketAcls[bucket] else {
                throw S3Error.noSuchBucket
            }
            return acl
        }
    }

    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy) async throws {
        if let key = key {
            if acls[bucket] == nil {
                acls[bucket] = [:]
            }
            acls[bucket]![key] = acl
        } else {
            bucketAcls[bucket] = acl
        }
    }

    // Policy methods
    func getBucketPolicy(bucket: String) async throws -> BucketPolicy {
        guard let policy = policies[bucket] else {
            throw S3Error.noSuchBucketPolicy
        }
        return policy
    }

    func putBucketPolicy(bucket: String, policy: BucketPolicy) async throws {
        policies[bucket] = policy
    }

    func deleteBucketPolicy(bucket: String) async throws {
        policies.removeValue(forKey: bucket)
    }

    // Versioning methods
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration? {
        return versioningConfigs[bucket]
    }

    func putBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws {
        versioningConfigs[bucket] = configuration
    }

    // Tagging methods
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag] {
        if let key = key {
            return tags[bucket]?[key] ?? []
        } else {
            return bucketTags[bucket] ?? []
        }
    }

    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws {
        if let key = key {
            if self.tags[bucket] == nil {
                self.tags[bucket] = [:]
            }
            self.tags[bucket]![key] = tags
        } else {
            bucketTags[bucket] = tags
        }
    }

    func deleteTags(bucket: String, key: String?, versionId: String?) async throws {
        if let key = key {
            tags[bucket]?[key] = []
        } else {
            bucketTags[bucket] = []
        }
    }

    // Lifecycle methods
    func getBucketLifecycle(bucket: String) async throws -> LifecycleConfiguration? {
        return lifecycleConfigs[bucket]
    }

    func putBucketLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws {
        lifecycleConfigs[bucket] = configuration
    }

    func deleteBucketLifecycle(bucket: String) async throws {
        lifecycleConfigs.removeValue(forKey: bucket)
    }

    // Multipart upload methods
    func createMultipartUpload(bucket: String, key: String, metadata: [String: String]?, owner: String) async throws -> String {
        throw S3Error.notImplemented
    }

    func uploadPart<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, uploadId: String, partNumber: Int, data: consuming Stream,
        size: Int64?
    ) async throws -> String where Stream.Element == ByteBuffer {
        throw S3Error.notImplemented
    }

    func completeMultipartUpload(bucket: String, key: String, uploadId: String, parts: [PartInfo]) async throws -> String {
        throw S3Error.notImplemented
    }

    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws {
        throw S3Error.notImplemented
    }

    func cleanupOrphanedUploads(olderThan: TimeInterval) async throws {
        // No-op for mock
    }

    func shutdown() async throws {
        // No-op for mock
    }
}