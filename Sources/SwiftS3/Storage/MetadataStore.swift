import Foundation
import NIO

/// Codable wrapper for any value type
struct AnyCodable: Codable {
    let value: Any

    init(_ value: Any) {
        self.value = value
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let string = try? container.decode(String.self) {
            value = string
        } else if let int = try? container.decode(Int.self) {
            value = int
        } else if let double = try? container.decode(Double.self) {
            value = double
        } else if let bool = try? container.decode(Bool.self) {
            value = bool
        } else if container.decodeNil() {
            value = NSNull()
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported type")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch value {
        case let string as String:
            try container.encode(string)
        case let int as Int:
            try container.encode(int)
        case let double as Double:
            try container.encode(double)
        case let bool as Bool:
            try container.encode(bool)
        case is NSNull:
            try container.encodeNil()
        default:
            throw EncodingError.invalidValue(value, EncodingError.Context(codingPath: encoder.codingPath, debugDescription: "Unsupported type"))
        }
    }
}

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

    /// Delete metadata for an object.
    /// Removes object metadata from the store. If versionId is specified, only that version is deleted.
    /// If versionId is nil, creates a delete marker for versioned buckets or removes the object entirely.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key
    ///   - versionId: Specific version to delete (nil for latest/current)
    /// - Throws: Error if object/version doesn't exist or deletion fails
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

    /// Shutdown the metadata store and release resources.
    /// Ensures all pending operations complete and connections are properly closed.
    /// Should be called when the application is terminating.
    ///
    /// - Throws: Error if shutdown fails
    func shutdown() async throws

    // ACLs
    /// Retrieve the access control policy for a bucket or object.
    /// Returns the ACL that defines permissions for the specified resource.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (nil for bucket ACL)
    ///   - versionId: Object version ID (nil for current version)
    /// - Returns: Access control policy with grants and owner information
    /// - Throws: Error if resource doesn't exist or access is denied
    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy

    /// Set the access control policy for a bucket or object.
    /// Updates the ACL with new grants and permissions for the specified resource.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (nil for bucket ACL)
    ///   - versionId: Object version ID (nil for current version)
    ///   - acl: New access control policy to apply
    /// - Throws: Error if update fails or access is denied
    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws

    // Versioning
    /// Get the versioning configuration for a bucket.
    /// Returns whether versioning is enabled, suspended, or not configured.
    ///
    /// - Parameter bucket: Bucket name
    /// - Returns: Versioning configuration or nil if not set
    /// - Throws: Error if bucket doesn't exist
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration?

    /// Set the versioning configuration for a bucket.
    /// Enables or suspends versioning for all objects in the bucket.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - configuration: New versioning configuration
    /// - Throws: Error if update fails
    func setBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws

    /// List all versions of objects in a bucket with optional filtering and pagination.
    /// Returns all versions (including delete markers) for versioned objects.
    /// Supports prefix filtering and pagination using key and version markers.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name to list versions from
    ///   - prefix: Key prefix filter
    ///   - delimiter: Grouping delimiter for hierarchical listing
    ///   - keyMarker: Key pagination marker
    ///   - versionIdMarker: Version ID pagination marker
    ///   - maxKeys: Maximum versions to return
    /// - Returns: ListVersionsResult with version information and pagination details
    /// - Throws: Error if bucket doesn't exist or query fails
    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult

    // Lifecycle
    /// Create a new bucket with the specified owner.
    /// Initializes bucket metadata and sets up default configurations.
    ///
    /// - Parameters:
    ///   - name: Bucket name
    ///   - owner: Owner identifier for the bucket
    /// - Throws: Error if bucket already exists or creation fails
    func createBucket(name: String, owner: String) async throws

    /// Delete a bucket and all its contents.
    /// Removes all objects, versions, and metadata associated with the bucket.
    ///
    /// - Parameter name: Bucket name to delete
    /// - Throws: Error if bucket doesn't exist, is not empty, or deletion fails
    func deleteBucket(name: String) async throws

    // Tagging
    /// Retrieve tags for a bucket or object.
    /// Returns all key-value tags associated with the specified resource.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (nil for bucket tags)
    ///   - versionId: Object version ID (nil for current version)
    /// - Returns: Array of S3 tags
    /// - Throws: Error if resource doesn't exist
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag]

    /// Set tags for a bucket or object.
    /// Replaces all existing tags with the new set of tags.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (nil for bucket tags)
    ///   - versionId: Object version ID (nil for current version)
    ///   - tags: Array of tags to set
    /// - Throws: Error if update fails
    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws

    /// Remove all tags from a bucket or object.
    /// Deletes all key-value tags associated with the specified resource.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (nil for bucket tags)
    ///   - versionId: Object version ID (nil for current version)
    /// - Throws: Error if deletion fails
    func deleteTags(bucket: String, key: String?, versionId: String?) async throws

    // Lifecycle
    /// Get the lifecycle configuration for a bucket.
    /// Returns rules for automatic object expiration and transitions.
    ///
    /// - Parameter bucket: Bucket name
    /// - Returns: Lifecycle configuration or nil if not set
    /// - Throws: Error if bucket doesn't exist
    func getLifecycle(bucket: String) async throws -> LifecycleConfiguration?

    /// Set the lifecycle configuration for a bucket.
    /// Defines rules for automatic object expiration, deletion, and storage class transitions.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - configuration: New lifecycle configuration
    /// - Throws: Error if update fails
    func putLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws

    /// Remove the lifecycle configuration from a bucket.
    /// Disables automatic lifecycle management for the bucket.
    ///
    /// - Parameter bucket: Bucket name
    /// - Throws: Error if deletion fails
    func deleteLifecycle(bucket: String) async throws

    // Object Lock
    /// Get the object lock configuration for a bucket.
    /// Returns retention and legal hold settings for the bucket.
    ///
    /// - Parameter bucket: Bucket name
    /// - Returns: Object lock configuration or nil if not enabled
    /// - Throws: Error if bucket doesn't exist
    func getObjectLockConfiguration(bucket: String) async throws -> ObjectLockConfiguration?

    /// Set the object lock configuration for a bucket.
    /// Enables object lock with specified retention and legal hold policies.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - configuration: New object lock configuration
    /// - Throws: Error if update fails or bucket already has object lock enabled
    func putObjectLockConfiguration(bucket: String, configuration: ObjectLockConfiguration) async throws

    // Replication
    /// Get the replication configuration for a bucket.
    /// Returns rules for cross-region replication of objects.
    ///
    /// - Parameter bucket: Bucket name
    /// - Returns: Replication configuration or nil if not set
    /// - Throws: Error if bucket doesn't exist
    func getBucketReplication(bucket: String) async throws -> ReplicationConfiguration?

    /// Set the replication configuration for a bucket.
    /// Enables automatic replication of objects to specified destination buckets.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - configuration: New replication configuration
    /// - Throws: Error if update fails
    func putBucketReplication(bucket: String, configuration: ReplicationConfiguration) async throws

    /// Remove the replication configuration from a bucket.
    /// Disables automatic replication for the bucket.
    ///
    /// - Parameter bucket: Bucket name
    /// - Throws: Error if deletion fails
    func deleteBucketReplication(bucket: String) async throws

    // Event Notifications
    func getBucketNotification(bucket: String) async throws -> NotificationConfiguration?
    func putBucketNotification(bucket: String, configuration: NotificationConfiguration) async throws
    func deleteBucketNotification(bucket: String) async throws

    // VPC-Only Access
    func getBucketVpcConfiguration(bucket: String) async throws -> VpcConfiguration?
    func putBucketVpcConfiguration(bucket: String, configuration: VpcConfiguration) async throws
    func deleteBucketVpcConfiguration(bucket: String) async throws

    // Advanced Auditing
    func logAuditEvent(_ event: AuditEvent) async throws
    func getAuditEvents(
        bucket: String?, principal: String?, eventType: AuditEventType?, startDate: Date?, endDate: Date?,
        limit: Int?, continuationToken: String?
    ) async throws -> (events: [AuditEvent], nextContinuationToken: String?)
    func deleteAuditEvents(olderThan: Date) async throws

    // Batch Operations
    func createBatchJob(job: BatchJob) async throws -> String
    func getBatchJob(jobId: String) async throws -> BatchJob?
    func listBatchJobs(bucket: String?, status: BatchJobStatus?, limit: Int?, continuationToken: String?) async throws -> (jobs: [BatchJob], nextContinuationToken: String?)
    func updateBatchJobStatus(jobId: String, status: BatchJobStatus, message: String?) async throws
    func deleteBatchJob(jobId: String) async throws
    func executeBatchOperation(jobId: String, bucket: String, key: String) async throws
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

        var customMetadata: [String: String] = [:]
        var contentType: String? = nil
        var storageClass: StorageClass = .standard
        var checksumAlgorithm: ChecksumAlgorithm? = nil
        var checksumValue: String? = nil
        var objectLockMode: ObjectLockMode? = nil
        var objectLockRetainUntilDate: Date? = nil
        var objectLockLegalHoldStatus: LegalHoldStatus? = nil
        var serverSideEncryption: ServerSideEncryptionConfig? = nil

        if FileManager.default.fileExists(atPath: metaPath),
            let data = try? Data(contentsOf: URL(fileURLWithPath: metaPath)),
            let dict = try? JSONDecoder().decode([String: AnyCodable].self, from: data)
        {
            // Extract simple string metadata
            for (key, value) in dict {
                if key == "Content-Type", let str = value.value as? String {
                    contentType = str
                } else if key == "storageClass", let str = value.value as? String {
                    storageClass = StorageClass(rawValue: str) ?? .standard
                } else if key == "checksumAlgorithm", let str = value.value as? String {
                    checksumAlgorithm = ChecksumAlgorithm(rawValue: str)
                } else if key == "checksumValue", let str = value.value as? String {
                    checksumValue = str
                } else if key == "objectLockMode", let str = value.value as? String {
                    objectLockMode = ObjectLockMode(rawValue: str)
                } else if key == "objectLockRetainUntilDate", let timestamp = value.value as? Double {
                    objectLockRetainUntilDate = Date(timeIntervalSince1970: timestamp)
                } else if key == "objectLockLegalHoldStatus", let str = value.value as? String {
                    objectLockLegalHoldStatus = LegalHoldStatus(rawValue: str)
                } else if key == "serverSideEncryptionAlgorithm", let str = value.value as? String,
                          let algorithm = ServerSideEncryption(rawValue: str) {
                    // For now, just store the algorithm. KMS fields would need more complex handling
                    serverSideEncryption = ServerSideEncryptionConfig(algorithm: algorithm)
                } else if let str = value.value as? String {
                    customMetadata[key] = str
                }
            }
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
            isDeleteMarker: false,
            storageClass: storageClass,
            checksumAlgorithm: checksumAlgorithm,
            checksumValue: checksumValue,
            objectLockMode: objectLockMode,
            objectLockRetainUntilDate: objectLockRetainUntilDate,
            objectLockLegalHoldStatus: objectLockLegalHoldStatus,
            serverSideEncryption: serverSideEncryption
        )
    }

    func saveMetadata(bucket: String, key: String, metadata: ObjectMetadata) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"

        // Merge all metadata into a dictionary for storage
        var dict: [String: AnyCodable] = [:]

        // Add custom metadata
        for (key, value) in metadata.customMetadata {
            dict[key] = AnyCodable(value)
        }

        // Add content type
        if let contentType = metadata.contentType {
            dict["Content-Type"] = AnyCodable(contentType)
        }

        // Add new fields
        dict["storageClass"] = AnyCodable(metadata.storageClass.rawValue)
        if let checksumAlgorithm = metadata.checksumAlgorithm {
            dict["checksumAlgorithm"] = AnyCodable(checksumAlgorithm.rawValue)
        }
        if let checksumValue = metadata.checksumValue {
            dict["checksumValue"] = AnyCodable(checksumValue)
        }
        if let objectLockMode = metadata.objectLockMode {
            dict["objectLockMode"] = AnyCodable(objectLockMode.rawValue)
        }
        if let objectLockRetainUntilDate = metadata.objectLockRetainUntilDate {
            dict["objectLockRetainUntilDate"] = AnyCodable(objectLockRetainUntilDate.timeIntervalSince1970)
        }
        if let objectLockLegalHoldStatus = metadata.objectLockLegalHoldStatus {
            dict["objectLockLegalHoldStatus"] = AnyCodable(objectLockLegalHoldStatus.rawValue)
        }
        if let serverSideEncryption = metadata.serverSideEncryption {
            dict["serverSideEncryptionAlgorithm"] = AnyCodable(serverSideEncryption.algorithm.rawValue)
            if let kmsKeyId = serverSideEncryption.kmsKeyId {
                dict["serverSideEncryptionKmsKeyId"] = AnyCodable(kmsKeyId)
            }
            if let kmsEncryptionContext = serverSideEncryption.kmsEncryptionContext {
                dict["serverSideEncryptionKmsEncryptionContext"] = AnyCodable(kmsEncryptionContext)
            }
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

    // MARK: - Object Lock

    func getObjectLockConfiguration(bucket: String) async throws -> ObjectLockConfiguration? {
        return nil // Not implemented for FileSystem
    }

    func putObjectLockConfiguration(bucket: String, configuration: ObjectLockConfiguration) async throws {
        // Not implemented for FileSystem
    }

    // MARK: - Replication

    func getBucketReplication(bucket: String) async throws -> ReplicationConfiguration? {
        return nil // Not implemented for FileSystem
    }

    func putBucketReplication(bucket: String, configuration: ReplicationConfiguration) async throws {
        // Not implemented for FileSystem
    }

    func deleteBucketReplication(bucket: String) async throws {
        // Not implemented for FileSystem
    }

    // MARK: - Event Notifications

    func getBucketNotification(bucket: String) async throws -> NotificationConfiguration? {
        return nil // Not implemented for FileSystem
    }

    func putBucketNotification(bucket: String, configuration: NotificationConfiguration) async throws {
        // Not implemented for FileSystem
    }

    func deleteBucketNotification(bucket: String) async throws {
        // Not implemented for FileSystem
    }

    // MARK: - VPC Configuration

    func getBucketVpcConfiguration(bucket: String) async throws -> VpcConfiguration? {
        return nil // Not implemented for FileSystem
    }

    func putBucketVpcConfiguration(bucket: String, configuration: VpcConfiguration) async throws {
        // Not implemented for FileSystem
    }

    func deleteBucketVpcConfiguration(bucket: String) async throws {
        // Not implemented for FileSystem
    }

    // MARK: - Audit Events

    func logAuditEvent(_ event: AuditEvent) async throws {
        // Not implemented for FileSystem - audit events require persistent storage
    }

    func getAuditEvents(
        bucket: String?, principal: String?, eventType: AuditEventType?, startDate: Date?, endDate: Date?,
        limit: Int?, continuationToken: String?
    ) async throws -> (events: [AuditEvent], nextContinuationToken: String?) {
        // Not implemented for FileSystem - audit events require persistent storage
        return (events: [], nextContinuationToken: nil)
    }

    func deleteAuditEvents(olderThan: Date) async throws {
        // Not implemented for FileSystem - audit events require persistent storage
    }

    // MARK: - Batch Operations

    func createBatchJob(job: BatchJob) async throws -> String {
        // Not implemented for FileSystem - batch operations require persistent storage
        throw S3Error(code: "NotImplemented", message: "Batch operations not supported with filesystem storage", statusCode: .notImplemented)
    }

    func getBatchJob(jobId: String) async throws -> BatchJob? {
        // Not implemented for FileSystem - batch operations require persistent storage
        return nil
    }

    func listBatchJobs(bucket: String?, status: BatchJobStatus?, limit: Int?, continuationToken: String?) async throws -> (jobs: [BatchJob], nextContinuationToken: String?) {
        // Not implemented for FileSystem - batch operations require persistent storage
        return (jobs: [], nextContinuationToken: nil)
    }

    func updateBatchJobStatus(jobId: String, status: BatchJobStatus, message: String?) async throws {
        // Not implemented for FileSystem - batch operations require persistent storage
    }

    func deleteBatchJob(jobId: String) async throws {
        // Not implemented for FileSystem - batch operations require persistent storage
    }

    func executeBatchOperation(jobId: String, bucket: String, key: String) async throws {
        // Not implemented for FileSystem - batch operations require persistent storage
    }
}
