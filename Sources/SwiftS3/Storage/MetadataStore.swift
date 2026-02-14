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

    // Object Lock
    func getObjectLockConfiguration(bucket: String) async throws -> ObjectLockConfiguration?
    func putObjectLockConfiguration(bucket: String, configuration: ObjectLockConfiguration) async throws

    // Replication
    func getBucketReplication(bucket: String) async throws -> ReplicationConfiguration?
    func putBucketReplication(bucket: String, configuration: ReplicationConfiguration) async throws
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
}
