import Foundation
import NIO

/// Protocol defining metadata operations for S3 objects.
/// Provides abstraction for storing and retrieving object metadata, ACLs, versioning info, and bucket configurations.
/// Implementations can use different backing stores (SQLite, in-memory, cloud databases) while maintaining consistent API.
/// All operations are async to support various storage backends with different performance characteristics.
protocol MetadataStore: Sendable {
    /// Retrieve metadata for an object.
    /// Resolves version IDs and handles delete marker logic.
    /// Returns the most recent version if versionId is nil.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key
    ///   - versionId: Specific version (nil for latest)
    /// - Returns: Complete object metadata
    /// - Throws: Error if object doesn't exist
    func getMetadata(bucket: String, key: String, versionId: String?) async throws -> ObjectMetadata

    /// Save metadata for an object.
    /// Creates or updates object metadata in the store.
    /// Handles versioning automatically based on bucket configuration.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key
    ///   - metadata: Complete object metadata to store
    /// - Throws: Error if storage operation fails
    func saveMetadata(bucket: String, key: String, metadata: ObjectMetadata) async throws

    /// Delete metadata for an object
    func deleteMetadata(bucket: String, key: String, versionId: String?) async throws

    /// List objects in a bucket with optional filtering and pagination.
    /// Supports prefix filtering, delimiter grouping, and continuation tokens.
    /// Returns results compatible with AWS S3 ListObjects APIs.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name to list
    ///   - prefix: Key prefix filter
    ///   - delimiter: Grouping delimiter for hierarchical listing
    ///   - marker: Pagination marker
    ///   - continuationToken: Alternative pagination token
    ///   - maxKeys: Maximum objects to return
    /// - Returns: ListObjectsResult with objects and pagination info
    /// - Throws: Error if bucket doesn't exist or query fails
    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult

    func shutdown() async throws

    // ACLs
    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy
    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws

    // Versioning
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration?
    func setBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws

    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult

    // Lifecycle
    func createBucket(name: String, owner: String) async throws
    func deleteBucket(name: String) async throws

    // Tagging
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag]
    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws
    func deleteTags(bucket: String, key: String?, versionId: String?) async throws

    // Lifecycle
    func getLifecycle(bucket: String) async throws -> LifecycleConfiguration?
    func putLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws
    func deleteLifecycle(bucket: String) async throws
}

/// Default implementation storing metadata in sidecar JSON files
struct FileSystemMetadataStore: MetadataStore {
    let rootPath: String

    init(rootPath: String) {
        self.rootPath = rootPath
    }

    private func getObjectPath(bucket: String, key: String) -> String {
        return "\(rootPath)/\(bucket)/\(key)"
    }

    func getMetadata(bucket: String, key: String, versionId: String?) async throws -> ObjectMetadata
    {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"

        guard FileManager.default.fileExists(atPath: path) else {
            throw S3Error.noSuchKey
        }

        // Read basic file attributes
        let attr = try FileManager.default.attributesOfItem(atPath: path)
        let size = attr[.size] as? Int64 ?? 0
        let date = attr[.modificationDate] as? Date ?? Date()

        // Read Custom Metadata from sidecar file
        var customMetadata: [String: String] = [:]
        var contentType: String? = nil

        if FileManager.default.fileExists(atPath: metaPath),
            let data = try? Data(contentsOf: URL(fileURLWithPath: metaPath)),
            let dict = try? JSONDecoder().decode([String: String].self, from: data)
        {
            customMetadata = dict
            contentType = dict["Content-Type"]
        }

        return ObjectMetadata(
            key: key,
            size: size,
            lastModified: date,
            eTag: nil,  // ETag generation/storage can be improved later
            contentType: contentType,
            customMetadata: customMetadata,
            versionId: "null",
            isLatest: true,
            isDeleteMarker: false
        )
    }

    func saveMetadata(bucket: String, key: String, metadata: ObjectMetadata) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"

        // Merge Content-Type into custom metadata for storage (simulating current behavior)
        var dict = metadata.customMetadata
        if let contentType = metadata.contentType {
            dict["Content-Type"] = contentType
        }

        let data = try JSONEncoder().encode(dict)
        try data.write(to: URL(fileURLWithPath: metaPath))
    }

    func deleteMetadata(bucket: String, key: String, versionId: String?) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"

        if FileManager.default.fileExists(atPath: metaPath) {
            try FileManager.default.removeItem(atPath: metaPath)
        }
    }

    private func bucketPath(_ name: String) -> String {
        return "\(rootPath)/\(name)"
    }

    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult {
        let bPath = bucketPath(bucket)
        if !FileManager.default.fileExists(atPath: bPath) {
            throw S3Error.noSuchBucket
        }

        let bucketURL = URL(fileURLWithPath: bPath).standardizedFileURL
        let bucketPathString = bucketURL.path

        let enumerator = FileManager.default.enumerator(
            at: bucketURL, includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey],
            options: [.skipsHiddenFiles])

        var allObjects: [ObjectMetadata] = []

        while let url = enumerator?.nextObject() as? URL {
            guard try url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory != true else {
                continue
            }

            let standardURL = url.standardizedFileURL
            let fullPath = standardURL.path

            guard fullPath.hasPrefix(bucketPathString) else {
                continue
            }

            var relativeKey = String(fullPath.dropFirst(bucketPathString.count))
            if relativeKey.hasPrefix("/") {
                relativeKey.removeFirst()
            }

            // Filter by Prefix
            if let prefix = prefix, !relativeKey.hasPrefix(prefix) {
                continue
            }

            // Ignore internal metadata files
            if relativeKey.hasSuffix(".metadata") || relativeKey.contains("/.uploads/") {
                continue
            }

            // Filter by Marker (V1) or ContinuationToken (V2)
            let startAfter = continuationToken ?? marker
            if let startAfter = startAfter, relativeKey <= startAfter {
                continue
            }

            let values = try url.resourceValues(forKeys: [
                .fileSizeKey, .contentModificationDateKey,
            ])

            allObjects.append(
                ObjectMetadata(
                    key: relativeKey,
                    size: Int64(values.fileSize ?? 0),
                    lastModified: values.contentModificationDate ?? Date(),
                    eTag: nil
                ))
        }

        // Sort by Key
        allObjects.sort { $0.key < $1.key }

        // Apply Delimiter (Grouping)
        var objects: [ObjectMetadata] = []
        var commonPrefixes: Set<String> = []
        var truncated = false
        var nextMarker: String? = nil
        var nextContinuationToken: String? = nil

        let limit = maxKeys ?? 1000
        var count = 0

        // We need to keep track of the last seen "rolled up" prefix to avoid duplicates in the run
        var lastPrefix: String? = nil

        for obj in allObjects {
            if count >= limit {
                truncated = true
                nextMarker = objects.last?.key
                nextContinuationToken = objects.last?.key  // Use key as token for now
                break
            }

            let key = obj.key
            var isCommonPrefix = false
            var currentPrefix = ""

            if let delimiter = delimiter {
                let prefixLen = prefix?.count ?? 0
                let searchRange = key.index(key.startIndex, offsetBy: prefixLen)..<key.endIndex

                if let range = key.range(of: delimiter, range: searchRange) {
                    currentPrefix = String(key[..<range.upperBound])
                    isCommonPrefix = true
                }
            }

            if isCommonPrefix {
                if currentPrefix != lastPrefix {
                    commonPrefixes.insert(currentPrefix)
                    lastPrefix = currentPrefix
                    count += 1
                }
            } else {
                objects.append(obj)
                count += 1
            }
        }

        return ListObjectsResult(
            objects: objects,
            commonPrefixes: Array(commonPrefixes).sorted(),
            isTruncated: truncated,
            nextMarker: nextMarker,
            nextContinuationToken: nextContinuationToken
        )
    }

    func shutdown() async throws {
        // No-op for file system store
    }

    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy
    {
        let path: String
        if let key = key {
            path = getObjectPath(bucket: bucket, key: key) + ".acl"
        } else {
            path = bucketPath(bucket) + "/.bucket_acl"
        }

        guard FileManager.default.fileExists(atPath: path),
            let data = try? Data(contentsOf: URL(fileURLWithPath: path)),
            let acl = try? JSONDecoder().decode(AccessControlPolicy.self, from: data)
        else {
            // Default to private if no ACL exists
            // For now, we return a dummy or throw. S3 defaults to private (owner full control).
            // We need the owner ID to create a default ACL.
            // Since we don't track owner in FS store easily without sidecars, we might throw or return a default.
            // Let's return a default "unknown" owner for now or throw.
            // Better: throw no such ACL or return a default logic in Controller if not found.
            // Standard S3 behaviour: every object has an ACL.
            // We will throw specific error so Controller can generate default.
            throw S3Error.noSuchKey  // Or similar
        }
        return acl
    }

    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws
    {
        let path: String
        if let key = key {
            path = getObjectPath(bucket: bucket, key: key) + ".acl"
        } else {
            path = bucketPath(bucket) + "/.bucket_acl"
        }

        let data = try JSONEncoder().encode(acl)
        try data.write(to: URL(fileURLWithPath: path))
    }

    // MARK: - Versioning

    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration? {
        let path = bucketPath(bucket) + "/versioning.json"

        guard FileManager.default.fileExists(atPath: path) else {
            return VersioningConfiguration(status: .suspended)
        }

        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        return try JSONDecoder().decode(VersioningConfiguration.self, from: data)
    }

    func setBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws {
        let path = bucketPath(bucket) + "/versioning.json"
        let data = try JSONEncoder().encode(configuration)
        try data.write(to: URL(fileURLWithPath: path))
    }

    func createBucket(name: String, owner: String) async throws {
        // Create sidecar for bucket metadata (owner)
        let path = bucketPath(name)
        // We assume directory exists or will be created by Storage
        // But we need to write .bucket_metadata
        // If directory doesn't exist, this fails.
        // FileSystemStorage should create directory first.
        // We will just write the file.
        let metaPath = path + "/.bucket_metadata"
        let metadata = ["owner": owner]
        let data = try JSONEncoder().encode(metadata)
        try data.write(to: URL(fileURLWithPath: metaPath))
    }

    func deleteBucket(name: String) async throws {
        let path = bucketPath(name) + "/.bucket_metadata"
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(atPath: path)
        }
        let aclPath = bucketPath(name) + "/.bucket_acl"
        if FileManager.default.fileExists(atPath: aclPath) {
            try FileManager.default.removeItem(atPath: aclPath)
        }
    }

    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult {
        // FS Store doesn't support real versioning yet, so we just list objects as latest versions
        let objectsResult = try await listObjects(
            bucket: bucket, prefix: prefix, delimiter: delimiter, marker: keyMarker,
            continuationToken: nil, maxKeys: maxKeys)

        let versions = objectsResult.objects.map { obj in
            ObjectMetadata(
                key: obj.key, size: obj.size, lastModified: obj.lastModified, eTag: obj.eTag,
                contentType: obj.contentType, customMetadata: obj.customMetadata, owner: obj.owner,
                versionId: "null", isLatest: true, isDeleteMarker: false)
        }

        return ListVersionsResult(
            versions: versions,
            commonPrefixes: objectsResult.commonPrefixes,
            isTruncated: objectsResult.isTruncated,
            nextKeyMarker: objectsResult.nextMarker,
            nextVersionIdMarker: nil
        )
    }

    // Tagging (No-op or partial for FS)
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag] {
        return []
    }

    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws {
        // No-op for now in FS store
    }

    func deleteTags(bucket: String, key: String?, versionId: String?) async throws {
        // No-op
    }

    // MARK: - Lifecycle

    func getLifecycle(bucket: String) async throws -> LifecycleConfiguration? {
        return nil
    }

    func putLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws {
        // Not implemented for FileSystem
    }

    func deleteLifecycle(bucket: String) async throws {
        // Not implemented for FileSystem
    }
}
