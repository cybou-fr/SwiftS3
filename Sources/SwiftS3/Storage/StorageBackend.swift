import Foundation
import Hummingbird

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

struct S3Tag: Sendable, Codable, Equatable {
    let key: String
    let value: String

    init(key: String, value: String) {
        self.key = key
        self.value = value
    }
}

struct PartInfo: Sendable, Codable {
    let partNumber: Int
    let eTag: String
}

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

protocol StorageBackend: Sendable {
    func listBuckets() async throws -> [(name: String, created: Date)]
    func createBucket(name: String, owner: String) async throws
    func deleteBucket(name: String) async throws
    func headBucket(name: String) async throws

    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    )
        async throws -> ListObjectsResult

    func copyObject(
        fromBucket: String, fromKey: String, toBucket: String, toKey: String, owner: String
    )
        async throws -> ObjectMetadata

    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: consuming Stream, size: Int64?,
        metadata: [String: String]?, owner: String
    ) async throws -> ObjectMetadata where Stream.Element == ByteBuffer
    func getObject(bucket: String, key: String, versionId: String?, range: ValidatedRange?)
        async throws -> (
            metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?
        )
    func deleteObject(bucket: String, key: String, versionId: String?) async throws -> (
        versionId: String?, isDeleteMarker: Bool
    )
    func deleteObjects(bucket: String, keys: [String]) async throws -> [String]  // TODO: Support versioned bulk delete
    func getObjectMetadata(bucket: String, key: String, versionId: String?) async throws
        -> ObjectMetadata

    // Multipart Upload
    func createMultipartUpload(
        bucket: String, key: String, metadata: [String: String]?, owner: String
    )
        async throws -> String
    func uploadPart<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, uploadId: String, partNumber: Int, data: consuming Stream,
        size: Int64?
    ) async throws -> String where Stream.Element == ByteBuffer
    func completeMultipartUpload(bucket: String, key: String, uploadId: String, parts: [PartInfo])
        async throws -> String
    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws

    // Bucket Policy
    func getBucketPolicy(bucket: String) async throws -> BucketPolicy
    func putBucketPolicy(bucket: String, policy: BucketPolicy) async throws
    func deleteBucketPolicy(bucket: String) async throws

    // ACLs
    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy
    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws

    // Versioning
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration?
    func putBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws

    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult

    // Tagging
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag]
    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws
    func deleteTags(bucket: String, key: String?, versionId: String?) async throws

    // Lifecycle
    func getBucketLifecycle(bucket: String) async throws -> LifecycleConfiguration?
    func putBucketLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws
    func deleteBucketLifecycle(bucket: String) async throws
}

public struct VersioningConfiguration: Codable, Sendable {
    public enum Status: String, Codable, Sendable {
        case enabled = "Enabled"
        case suspended = "Suspended"
    }
    public let status: Status

    public init(status: Status) {
        self.status = status
    }
}

public struct LifecycleConfiguration: Codable, Sendable {
    public struct Rule: Codable, Sendable {
        public enum Status: String, Codable, Sendable {
            case enabled = "Enabled"
            case disabled = "Disabled"
        }

        public struct Filter: Codable, Sendable {
            public let prefix: String?
            // Tag filter could be added later
            public init(prefix: String?) {
                self.prefix = prefix
            }
        }

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

        public init(id: String?, status: Status, filter: Filter, expiration: Expiration?) {
            self.id = id
            self.status = status
            self.filter = filter
            self.expiration = expiration
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
