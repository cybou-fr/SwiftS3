import Foundation
import Hummingbird

/// Metadata associated with an S3 object, including size, timestamps, and ownership.
/// Contains all the information needed to describe an object without its actual data.
/// Used for listing operations, HEAD requests, and metadata queries.
struct ObjectMetadata: Sendable {
    let key: String
    let size: Int64
    var lastModified: Date
    let eTag: String?
    let contentType: String?
    let customMetadata: [String: String]
    let owner: String?
    let versionId: String
    let isLatest: Bool
    let isDeleteMarker: Bool

    init(
        key: String, size: Int64, lastModified: Date, eTag: String?, contentType: String? = nil,
        customMetadata: [String: String] = [:], owner: String? = nil,
        versionId: String = "null", isLatest: Bool = true, isDeleteMarker: Bool = false
    ) {
        self.key = key
        self.size = size
        self.lastModified = lastModified
        self.eTag = eTag
        self.contentType = contentType
        self.customMetadata = customMetadata
        self.owner = owner
        self.versionId = versionId
        self.isLatest = isLatest
        self.isDeleteMarker = isDeleteMarker
    }
}

/// Represents a key-value tag for S3 objects or buckets.
/// Tags are metadata labels that can be attached to resources for organization,
/// cost tracking, and access control purposes. Each resource can have up to 10 tags.
public struct S3Tag: Sendable, Codable, Equatable {
    let key: String
    let value: String

    init(key: String, value: String) {
        self.key = key
        self.value = value
    }
}

/// Information about a part in a multipart upload.
/// Used to track individual chunks of data in large object uploads.
/// Each part has a sequential number and an ETag for integrity verification.
struct PartInfo: Sendable, Codable {
    let partNumber: Int
    let eTag: String
}

/// Result of a list objects operation, including pagination information.
struct ListObjectsResult: Sendable {
    let objects: [ObjectMetadata]
    let commonPrefixes: [String]
    let isTruncated: Bool
    let nextMarker: String?
    let nextContinuationToken: String?

    init(
        objects: [ObjectMetadata], commonPrefixes: [String], isTruncated: Bool,
        nextMarker: String? = nil, nextContinuationToken: String? = nil
    ) {
        self.objects = objects
        self.commonPrefixes = commonPrefixes
        self.isTruncated = isTruncated
        self.nextMarker = nextMarker
        self.nextContinuationToken = nextContinuationToken
    }
}

struct ListVersionsResult: Sendable {
    let versions: [ObjectMetadata]
    let commonPrefixes: [String]
    let isTruncated: Bool
    let nextKeyMarker: String?
    let nextVersionIdMarker: String?

    init(
        versions: [ObjectMetadata], commonPrefixes: [String], isTruncated: Bool,
        nextKeyMarker: String? = nil, nextVersionIdMarker: String? = nil
    ) {
        self.versions = versions
        self.commonPrefixes = commonPrefixes
        self.isTruncated = isTruncated
        self.nextKeyMarker = nextKeyMarker
        self.nextVersionIdMarker = nextVersionIdMarker
    }
}

/// Protocol defining the interface for S3-compatible storage backends.
/// Implementations provide persistent storage for buckets, objects, and metadata.
/// This abstraction allows SwiftS3 to support different storage engines (file system, cloud storage, etc.)
/// while maintaining a consistent API for the S3 controller layer.
protocol StorageBackend: Sendable {
    /// Lists all buckets in the storage system.
    func listBuckets() async throws -> [(name: String, created: Date)]
    /// Creates a new bucket with the specified name and owner.
    func createBucket(name: String, owner: String) async throws
    /// Deletes the specified bucket.
    func deleteBucket(name: String) async throws
    /// Checks if a bucket exists and returns metadata about it.
    func headBucket(name: String) async throws

    /// Lists objects in a bucket with optional filtering and pagination.
    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    )
        async throws -> ListObjectsResult

    /// Copies an object from one location to another within the storage system.
    func copyObject(
        fromBucket: String, fromKey: String, toBucket: String, toKey: String, owner: String
    )
        async throws -> ObjectMetadata

    /// Stores a new object in the specified bucket.
    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: consuming Stream, size: Int64?,
        metadata: [String: String]?, owner: String
    ) async throws -> ObjectMetadata where Stream.Element == ByteBuffer
    /// Retrieves an object and its metadata from storage.
    func getObject(bucket: String, key: String, versionId: String?, range: ValidatedRange?)
        async throws -> (
            metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?
        )
    /// Deletes an object from the specified bucket.
    func deleteObject(bucket: String, key: String, versionId: String?) async throws -> (
        versionId: String?, isDeleteMarker: Bool
    )
    /// Deletes multiple objects from the specified bucket.
    func deleteObjects(bucket: String, keys: [String]) async throws -> [String]  // TODO: Support versioned bulk delete
    /// Retrieves metadata for an object without its body.
    func getObjectMetadata(bucket: String, key: String, versionId: String?) async throws
        -> ObjectMetadata

    // Multipart Upload
    /// Initiates a multipart upload for an object.
    func createMultipartUpload(
        bucket: String, key: String, metadata: [String: String]?, owner: String
    )
        async throws -> String
    /// Uploads a part of a multipart upload.
    func uploadPart<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, uploadId: String, partNumber: Int, data: consuming Stream,
        size: Int64?
    ) async throws -> String where Stream.Element == ByteBuffer
    /// Completes a multipart upload by assembling all parts.
    func completeMultipartUpload(bucket: String, key: String, uploadId: String, parts: [PartInfo])
        async throws -> String
    /// Aborts an incomplete multipart upload.
    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws

    // Bucket Policy
    /// Retrieves the bucket policy for the specified bucket.
    func getBucketPolicy(bucket: String) async throws -> BucketPolicy
    /// Sets the bucket policy for the specified bucket.
    func putBucketPolicy(bucket: String, policy: BucketPolicy) async throws
    /// Deletes the bucket policy for the specified bucket.
    func deleteBucketPolicy(bucket: String) async throws

    // ACLs
    /// Retrieves the access control list for a bucket or object.
    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy
    /// Sets the access control list for a bucket or object.
    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws

    // Versioning
    /// Retrieves the versioning configuration for a bucket.
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration?
    /// Sets the versioning configuration for a bucket.
    func putBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws

    /// Lists all versions of objects in a bucket.
    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult

    // Tagging
    /// Retrieves tags for a bucket or object.
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag]
    /// Sets tags for a bucket or object.
    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws
    /// Deletes tags from a bucket or object.
    func deleteTags(bucket: String, key: String?, versionId: String?) async throws

    // Lifecycle
    /// Retrieves the lifecycle configuration for a bucket.
    func getBucketLifecycle(bucket: String) async throws -> LifecycleConfiguration?
    /// Sets the lifecycle configuration for a bucket.
    func putBucketLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws
    /// Deletes the lifecycle configuration for a bucket.
    func deleteBucketLifecycle(bucket: String) async throws

    // Garbage collection
    /// Cleans up orphaned multipart uploads older than the specified time interval.
    func cleanupOrphanedUploads(olderThan: TimeInterval) async throws
}

/// Configuration for bucket versioning behavior.
public struct VersioningConfiguration: Codable, Sendable {
    /// The versioning status of the bucket.
    public enum Status: String, Codable, Sendable {
        case enabled = "Enabled"
        case suspended = "Suspended"
    }
    public let status: Status
    public let mfaDelete: Bool?

    public init(status: Status, mfaDelete: Bool? = nil) {
        self.status = status
        self.mfaDelete = mfaDelete
    }
}

/// Configuration for bucket lifecycle management rules.
public struct LifecycleConfiguration: Codable, Sendable {
    /// A single lifecycle rule defining when objects should be expired or transitioned.
    public struct Rule: Codable, Sendable {
        /// Whether the rule is enabled or disabled.
        public enum Status: String, Codable, Sendable {
            case enabled = "Enabled"
            case disabled = "Disabled"
        }

        /// Filter criteria for objects affected by this rule.
        public struct Filter: Codable, Sendable {
            public let prefix: String?
            public let tag: S3Tag?

            public init(prefix: String?, tag: S3Tag? = nil) {
                self.prefix = prefix
                self.tag = tag
            }
        }

        /// Configuration for expiring non-current versions of objects.
        public struct NoncurrentVersionExpiration: Codable, Sendable {
            public let noncurrentDays: Int?
            public let newerNoncurrentVersions: Int?

            public init(noncurrentDays: Int? = nil, newerNoncurrentVersions: Int? = nil) {
                self.noncurrentDays = noncurrentDays
                self.newerNoncurrentVersions = newerNoncurrentVersions
            }
        }

        /// Configuration for when objects should expire.
        public struct Expiration: Codable, Sendable {
            public let days: Int?
            public let date: Date?
            public let expiredObjectDeleteMarker: Bool?

            public init(days: Int? = nil, date: Date? = nil, expiredObjectDeleteMarker: Bool? = nil)
            {
                self.days = days
                self.date = date
                self.expiredObjectDeleteMarker = expiredObjectDeleteMarker
            }
        }

        public let id: String?
        public let status: Status
        public let filter: Filter
        public let expiration: Expiration?
        public let noncurrentVersionExpiration: NoncurrentVersionExpiration?

        public init(id: String?, status: Status, filter: Filter, expiration: Expiration?, noncurrentVersionExpiration: NoncurrentVersionExpiration?) {
            self.id = id
            self.status = status
            self.filter = filter
            self.expiration = expiration
            self.noncurrentVersionExpiration = noncurrentVersionExpiration
        }
    }

    public let rules: [Rule]

    public init(rules: [Rule]) {
        self.rules = rules
    }
}

struct ValidatedRange: Sendable {
    let start: Int64
    let end: Int64
}
