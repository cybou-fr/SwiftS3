import Foundation
import Hummingbird

/// Metadata associated with an S3 object, including size, timestamps, and ownership.
/// Contains all the information needed to describe an object without its actual data.
/// Used for listing operations, HEAD requests, and metadata queries.
struct ObjectMetadata {
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
    var storageClass: StorageClass
    var checksumAlgorithm: ChecksumAlgorithm?
    var checksumValue: String?
    var objectLockMode: ObjectLockMode?
    var objectLockRetainUntilDate: Date?
    var objectLockLegalHoldStatus: LegalHoldStatus?
    var serverSideEncryption: ServerSideEncryptionConfig?

    init(
        key: String, size: Int64, lastModified: Date, eTag: String?, contentType: String? = nil,
        customMetadata: [String: String] = [:], owner: String? = nil,
        versionId: String = "null", isLatest: Bool = true, isDeleteMarker: Bool = false,
        storageClass: StorageClass = .standard, checksumAlgorithm: ChecksumAlgorithm? = nil,
        checksumValue: String? = nil, objectLockMode: ObjectLockMode? = nil,
        objectLockRetainUntilDate: Date? = nil, objectLockLegalHoldStatus: LegalHoldStatus? = nil,
        serverSideEncryption: ServerSideEncryptionConfig? = nil
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
        self.storageClass = storageClass
        self.checksumAlgorithm = checksumAlgorithm
        self.checksumValue = checksumValue
        self.objectLockMode = objectLockMode
        self.objectLockRetainUntilDate = objectLockRetainUntilDate
        self.objectLockLegalHoldStatus = objectLockLegalHoldStatus
        self.serverSideEncryption = serverSideEncryption
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

/// Storage class for objects, determining performance and cost characteristics.
public enum StorageClass: String, Codable, Sendable {
    case standard = "STANDARD"
    case reducedRedundancy = "REDUCED_REDUNDANCY"
    case standardIa = "STANDARD_IA"
    case oneZoneIa = "ONEZONE_IA"
    case intelligentTiering = "INTELLIGENT_TIERING"
    case glacier = "GLACIER"
    case deepArchive = "DEEP_ARCHIVE"
    case outposts = "OUTPOSTS"
}

/// Checksum algorithms supported for data integrity verification.
public enum ChecksumAlgorithm: String, Codable, Sendable {
    case crc32 = "CRC32"
    case crc32c = "CRC32C"
    case sha1 = "SHA1"
    case sha256 = "SHA256"
}

/// Object lock modes for WORM (Write Once Read Many) compliance.
public enum ObjectLockMode: String, Codable, Sendable {
    case governance = "GOVERNANCE"
    case compliance = "COMPLIANCE"
}

/// Legal hold status for objects under legal hold.
public enum LegalHoldStatus: String, Codable, Sendable {
    case on = "ON"
    case off = "OFF"
}

/// Server-side encryption methods supported.
public enum ServerSideEncryption: String, Codable, Sendable {
    case aes256 = "AES256"
    case awsKms = "aws:kms"
}

/// Server-side encryption configuration for objects.
public struct ServerSideEncryptionConfig: Codable, Sendable {
    public let algorithm: ServerSideEncryption
    public let kmsKeyId: String?
    public let kmsEncryptionContext: String?

    public init(algorithm: ServerSideEncryption, kmsKeyId: String? = nil, kmsEncryptionContext: String? = nil) {
        self.algorithm = algorithm
        self.kmsKeyId = kmsKeyId
        self.kmsEncryptionContext = kmsEncryptionContext
    }
}

/// Replication rule defining how objects are replicated to destination regions.
public struct ReplicationRule: Codable, Sendable {
    public let id: String
    public let status: ReplicationStatus
    public let destination: ReplicationDestination
    public let filter: ReplicationFilter?

    public init(id: String, status: ReplicationStatus, destination: ReplicationDestination, filter: ReplicationFilter? = nil) {
        self.id = id
        self.status = status
        self.destination = destination
        self.filter = filter
    }
}

/// Destination configuration for replication.
public struct ReplicationDestination: Codable, Sendable {
    public let region: String
    public let bucket: String
    public let storageClass: StorageClass?

    public init(region: String, bucket: String, storageClass: StorageClass? = nil) {
        self.region = region
        self.bucket = bucket
        self.storageClass = storageClass
    }
}

/// Filter for replication rules.
public struct ReplicationFilter: Codable, Sendable {
    public let prefix: String?

    public init(prefix: String? = nil) {
        self.prefix = prefix
    }
}

/// Replication configuration for a bucket.
public struct ReplicationConfiguration: Codable, Sendable {
    public let role: String
    public let rules: [ReplicationRule]

    public init(role: String, rules: [ReplicationRule]) {
        self.role = role
        self.rules = rules
    }
}

/// Status of replication for an object.
public enum ReplicationStatus: String, Codable, Sendable {
    case pending = "PENDING"
    case completed = "COMPLETED"
    case failed = "FAILED"
}

/// VPC configuration for restricting bucket access to specific networks.
public struct VpcConfiguration: Codable, Sendable {
    /// VPC ID for the configuration (optional, for AWS compatibility)
    public let vpcId: String?
    /// List of allowed IP ranges in CIDR notation (e.g., ["10.0.0.0/8", "192.168.1.0/24"])
    public let allowedIpRanges: [String]

    public init(vpcId: String? = nil, allowedIpRanges: [String]) {
        self.vpcId = vpcId
        self.allowedIpRanges = allowedIpRanges
    }
}

/// Batch operations configuration and job management.
/// Supports large-scale operations on S3 objects like copying, tagging, and deleting.
public struct BatchJob: Codable, Sendable {
    public let id: String
    public let operation: BatchOperation
    public let manifest: BatchManifest
    public let priority: Int
    public let roleArn: String?
    public let status: BatchJobStatus
    public let createdAt: Date
    public let completedAt: Date?
    public let failureReasons: [String]
    public let progress: BatchProgress

    public init(
        id: String = UUID().uuidString,
        operation: BatchOperation,
        manifest: BatchManifest,
        priority: Int = 0,
        roleArn: String? = nil,
        status: BatchJobStatus = .pending,
        createdAt: Date = Date(),
        completedAt: Date? = nil,
        failureReasons: [String] = [],
        progress: BatchProgress = BatchProgress()
    ) {
        self.id = id
        self.operation = operation
        self.manifest = manifest
        self.priority = priority
        self.roleArn = roleArn
        self.status = status
        self.createdAt = createdAt
        self.completedAt = completedAt
        self.failureReasons = failureReasons
        self.progress = progress
    }
}

public struct BatchManifest: Codable, Sendable {
    public let location: BatchManifestLocation
    public let spec: BatchManifestSpec

    public init(location: BatchManifestLocation, spec: BatchManifestSpec) {
        self.location = location
        self.spec = spec
    }
}

public struct BatchManifestLocation: Codable, Sendable {
    public let bucket: String
    public let key: String
    public let etag: String?

    public init(bucket: String, key: String, etag: String? = nil) {
        self.bucket = bucket
        self.key = key
        self.etag = etag
    }
}

public struct BatchManifestSpec: Codable, Sendable {
    public let format: BatchManifestFormat
    public let fields: [String]

    public init(format: BatchManifestFormat, fields: [String]) {
        self.format = format
        self.fields = fields
    }
}

public enum BatchManifestFormat: String, Codable, Sendable {
    case s3BatchOperationsCsv20180820 = "S3BatchOperations_CSV_20180820"
    case s3InventoryReportCsv20161130 = "S3InventoryReport_CSV_20161130"
}

public struct BatchOperation: Codable, Sendable {
    public let type: BatchOperationType
    public let parameters: [String: String]

    public init(type: BatchOperationType, parameters: [String: String] = [:]) {
        self.type = type
        self.parameters = parameters
    }
}

public enum BatchOperationType: String, Codable, Sendable {
    case lambdaInvoke = "LambdaInvoke"
    case s3PutObjectCopy = "S3PutObjectCopy"
    case s3PutObjectAcl = "S3PutObjectAcl"
    case s3PutObjectTagging = "S3PutObjectTagging"
    case s3DeleteObject = "S3DeleteObject"
    case s3InitiateRestoreObject = "S3InitiateRestoreObject"
    case s3PutObjectLegalHold = "S3PutObjectLegalHold"
    case s3PutObjectRetention = "S3PutObjectRetention"
}

public enum BatchJobStatus: String, Codable, Sendable {
    case pending = "Pending"
    case preparing = "Preparing"
    case ready = "Ready"
    case active = "Active"
    case paused = "Paused"
    case complete = "Complete"
    case cancelling = "Cancelling"
    case cancelled = "Cancelled"
    case failed = "Failed"
}

public struct BatchProgress: Codable, Sendable {
    public let totalObjects: Int
    public let processedObjects: Int
    public let failedObjects: Int

    public init(totalObjects: Int = 0, processedObjects: Int = 0, failedObjects: Int = 0) {
        self.totalObjects = totalObjects
        self.processedObjects = processedObjects
        self.failedObjects = failedObjects
    }
}

/// Audit event types for compliance logging.
public enum AuditEventType: String, Codable, Sendable {
    case bucketCreated = "BucketCreated"
    case bucketDeleted = "BucketDeleted"
    case objectUploaded = "ObjectUploaded"
    case objectDownloaded = "ObjectDownloaded"
    case objectDeleted = "ObjectDeleted"
    case objectCopied = "ObjectCopied"
    case policyUpdated = "PolicyUpdated"
    case aclUpdated = "ACLUpdated"
    case versioningUpdated = "VersioningUpdated"
    case lifecycleUpdated = "LifecycleUpdated"
    case replicationUpdated = "ReplicationUpdated"
    case notificationUpdated = "NotificationUpdated"
    case vpcConfigUpdated = "VpcConfigUpdated"
    case accessDenied = "AccessDenied"
    case authenticationFailed = "AuthenticationFailed"
}

/// Audit event record for compliance and security monitoring.
public struct AuditEvent: Codable, Sendable {
    public let id: String
    public let timestamp: Date
    public let eventType: AuditEventType
    public let principal: String
    public let sourceIp: String?
    public let userAgent: String?
    public let requestId: String
    public let bucket: String?
    public let key: String?
    public let operation: String
    public let status: String
    public let errorMessage: String?
    public let additionalData: [String: String]?

    public init(
        id: String = UUID().uuidString,
        timestamp: Date = Date(),
        eventType: AuditEventType,
        principal: String,
        sourceIp: String? = nil,
        userAgent: String? = nil,
        requestId: String,
        bucket: String? = nil,
        key: String? = nil,
        operation: String,
        status: String,
        errorMessage: String? = nil,
        additionalData: [String: String]? = nil
    ) {
        self.id = id
        self.timestamp = timestamp
        self.eventType = eventType
        self.principal = principal
        self.sourceIp = sourceIp
        self.userAgent = userAgent
        self.requestId = requestId
        self.bucket = bucket
        self.key = key
        self.operation = operation
        self.status = status
        self.errorMessage = errorMessage
        self.additionalData = additionalData
    }
}

/// S3 event types that can trigger notifications.
public enum S3EventType: String, Codable, Sendable {
    case objectCreated = "s3:ObjectCreated:*"
    case objectCreatedPut = "s3:ObjectCreated:Put"
    case objectCreatedPost = "s3:ObjectCreated:Post"
    case objectCreatedCopy = "s3:ObjectCreated:Copy"
    case objectCreatedCompleteMultipartUpload = "s3:ObjectCreated:CompleteMultipartUpload"
    case objectRemoved = "s3:ObjectRemoved:*"
    case objectRemovedDelete = "s3:ObjectRemoved:Delete"
    case objectRemovedDeleteMarkerCreated = "s3:ObjectRemoved:DeleteMarkerCreated"
    case objectRestore = "s3:ObjectRestore:*"
    case objectRestorePost = "s3:ObjectRestore:Post"
    case objectRestoreCompleted = "s3:ObjectRestore:Completed"
    case reducedRedundancyLostObject = "s3:ReducedRedundancyLostObject"
    case replication = "s3:Replication:*"
    case replicationOperationFailedReplication = "s3:Replication:OperationFailedReplication"
    case replicationOperationNotTracked = "s3:Replication:OperationNotTracked"
    case replicationOperationMissedThreshold = "s3:Replication:OperationMissedThreshold"
    case replicationOperationReplicatedAfterThreshold = "s3:Replication:OperationReplicatedAfterThreshold"
}

/// Configuration for event notifications on a bucket.
public struct NotificationConfiguration: Codable, Sendable {
    public let topicConfigurations: [TopicConfiguration]?
    public let queueConfigurations: [QueueConfiguration]?
    public let lambdaConfigurations: [LambdaConfiguration]?
    public let webhookConfigurations: [WebhookConfiguration]?

    public init(
        topicConfigurations: [TopicConfiguration]? = nil,
        queueConfigurations: [QueueConfiguration]? = nil,
        lambdaConfigurations: [LambdaConfiguration]? = nil,
        webhookConfigurations: [WebhookConfiguration]? = nil
    ) {
        self.topicConfigurations = topicConfigurations
        self.queueConfigurations = queueConfigurations
        self.lambdaConfigurations = lambdaConfigurations
        self.webhookConfigurations = webhookConfigurations
    }
}

/// Topic-based event notification configuration.
public struct TopicConfiguration: Codable, Sendable {
    public let id: String?
    public let topicArn: String
    public let events: [S3EventType]
    public let filter: NotificationFilter?

    public init(id: String? = nil, topicArn: String, events: [S3EventType], filter: NotificationFilter? = nil) {
        self.id = id
        self.topicArn = topicArn
        self.events = events
        self.filter = filter
    }
}

/// Queue-based event notification configuration.
public struct QueueConfiguration: Codable, Sendable {
    public let id: String?
    public let queueArn: String
    public let events: [S3EventType]
    public let filter: NotificationFilter?

    public init(id: String? = nil, queueArn: String, events: [S3EventType], filter: NotificationFilter? = nil) {
        self.id = id
        self.queueArn = queueArn
        self.events = events
        self.filter = filter
    }
}

/// Lambda function-based event notification configuration.
public struct LambdaConfiguration: Codable, Sendable {
    public let id: String?
    public let lambdaFunctionArn: String
    public let events: [S3EventType]
    public let filter: NotificationFilter?

    public init(id: String? = nil, lambdaFunctionArn: String, events: [S3EventType], filter: NotificationFilter? = nil) {
        self.id = id
        self.lambdaFunctionArn = lambdaFunctionArn
        self.events = events
        self.filter = filter
    }
}

/// Webhook-based event notification configuration.
public struct WebhookConfiguration: Codable, Sendable {
    public let id: String?
    public let url: String
    public let events: [S3EventType]
    public let filter: NotificationFilter?

    public init(id: String? = nil, url: String, events: [S3EventType], filter: NotificationFilter? = nil) {
        self.id = id
        self.url = url
        self.events = events
        self.filter = filter
    }
}

/// Filter for event notifications.
public struct NotificationFilter: Codable, Sendable {
    public let key: KeyFilter?

    public init(key: KeyFilter? = nil) {
        self.key = key
    }
}

/// Key-based filter for object keys.
public struct KeyFilter: Codable, Sendable {
    public let filterRules: [FilterRule]

    public init(filterRules: [FilterRule]) {
        self.filterRules = filterRules
    }
}

/// Individual filter rule for key filtering.
public struct FilterRule: Codable, Sendable {
    public let name: FilterRuleName
    public let value: String

    public init(name: FilterRuleName, value: String) {
        self.name = name
        self.value = value
    }
}

/// Filter rule names for key filtering.
public enum FilterRuleName: String, Codable, Sendable {
    case prefix = "prefix"
    case suffix = "suffix"
}

/// S3 event record containing details about the event.
public struct S3EventRecord: Codable, Sendable {
    public let eventVersion: String
    public let eventSource: String
    public let awsRegion: String
    public let eventTime: Date
    public let eventName: S3EventType
    public let userIdentity: UserIdentity
    public let requestParameters: RequestParameters
    public let responseElements: ResponseElements
    public let s3: S3Entity

    public init(
        eventVersion: String = "2.1",
        eventSource: String = "aws:s3",
        awsRegion: String = "us-east-1",
        eventTime: Date = Date(),
        eventName: S3EventType,
        userIdentity: UserIdentity,
        requestParameters: RequestParameters,
        responseElements: ResponseElements,
        s3: S3Entity
    ) {
        self.eventVersion = eventVersion
        self.eventSource = eventSource
        self.awsRegion = awsRegion
        self.eventTime = eventTime
        self.eventName = eventName
        self.userIdentity = userIdentity
        self.requestParameters = requestParameters
        self.responseElements = responseElements
        self.s3 = s3
    }
}

/// User identity information for the event.
public struct UserIdentity: Codable, Sendable {
    public let principalId: String

    public init(principalId: String) {
        self.principalId = principalId
    }
}

/// Request parameters for the event.
public struct RequestParameters: Codable, Sendable {
    public let sourceIPAddress: String

    public init(sourceIPAddress: String) {
        self.sourceIPAddress = sourceIPAddress
    }
}

/// Response elements for the event.
public struct ResponseElements: Codable, Sendable {
    public let xAmzRequestId: String
    public let xAmzId2: String

    public init(xAmzRequestId: String, xAmzId2: String) {
        self.xAmzRequestId = xAmzRequestId
        self.xAmzId2 = xAmzId2
    }
}

/// S3 entity information for the event.
public struct S3Entity: Codable, Sendable {
    public let s3SchemaVersion: String
    public let configurationId: String
    public let bucket: S3Bucket
    public let object: S3Object

    public init(s3SchemaVersion: String = "1.0", configurationId: String, bucket: S3Bucket, object: S3Object) {
        self.s3SchemaVersion = s3SchemaVersion
        self.configurationId = configurationId
        self.bucket = bucket
        self.object = object
    }
}

/// S3 bucket information for the event.
public struct S3Bucket: Codable, Sendable {
    public let name: String
    public let ownerIdentity: UserIdentity
    public let arn: String

    public init(name: String, ownerIdentity: UserIdentity, arn: String) {
        self.name = name
        self.ownerIdentity = ownerIdentity
        self.arn = arn
    }
}

/// S3 object information for the event.
public struct S3Object: Codable, Sendable {
    public let key: String
    public let size: Int64?
    public let eTag: String?
    public let versionId: String?
    public let sequencer: String

    public init(key: String, size: Int64?, eTag: String?, versionId: String?, sequencer: String) {
        self.key = key
        self.size = size
        self.eTag = eTag
        self.versionId = versionId
        self.sequencer = sequencer
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
    func deleteObjects(bucket: String, objects: [DeleteObject]) async throws -> [(key: String, versionId: String?, isDeleteMarker: Bool, deleteMarkerVersionId: String?)]
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
    /// Uploads a part by copying from an existing object.
    func uploadPartCopy(
        bucket: String, key: String, uploadId: String, partNumber: Int, copySource: String,
        range: ValidatedRange?
    ) async throws -> String
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

    // Advanced Storage & Data Protection
    /// Changes the storage class of an existing object.
    func changeStorageClass(bucket: String, key: String, versionId: String?, newStorageClass: StorageClass) async throws
    /// Puts an object lock configuration on a bucket.
    func putObjectLockConfiguration(bucket: String, configuration: ObjectLockConfiguration) async throws
    /// Gets the object lock configuration for a bucket.
    func getObjectLockConfiguration(bucket: String) async throws -> ObjectLockConfiguration?
    /// Puts an object lock on a specific object.
    func putObjectLock(bucket: String, key: String, versionId: String?, mode: ObjectLockMode, retainUntilDate: Date?) async throws
    /// Puts a legal hold on a specific object.
    func putObjectLegalHold(bucket: String, key: String, versionId: String?, status: LegalHoldStatus) async throws
    /// Verifies data integrity using checksums and detects bitrot.
    func verifyDataIntegrity(bucket: String, key: String, versionId: String?) async throws -> DataIntegrityResult
    /// Repairs data corruption if possible (for erasure coding or bitrot recovery).
    func repairDataCorruption(bucket: String, key: String, versionId: String?) async throws -> Bool

    // Server-Side Encryption
    /// Encrypts data using the specified server-side encryption configuration.
    func encryptData(_ data: Data, with config: ServerSideEncryptionConfig) async throws -> (encryptedData: Data, key: Data?, iv: Data?)
    /// Decrypts data using the specified server-side encryption configuration.
    func decryptData(_ encryptedData: Data, with config: ServerSideEncryptionConfig, key: Data?, iv: Data?) async throws -> Data

    // Cross-Region Replication
    /// Configures cross-region replication for a bucket.
    func putBucketReplication(bucket: String, configuration: ReplicationConfiguration) async throws
    /// Gets the replication configuration for a bucket.
    func getBucketReplication(bucket: String) async throws -> ReplicationConfiguration?
    /// Deletes the replication configuration for a bucket.
    func deleteBucketReplication(bucket: String) async throws
    /// Replicates an object to configured destination regions.
    func replicateObject(bucket: String, key: String, versionId: String?, metadata: ObjectMetadata, data: Data) async throws
    /// Gets the replication status of an object.
    func getReplicationStatus(bucket: String, key: String, versionId: String?) async throws -> ReplicationStatus

    // Event Notifications
    /// Configures event notifications for a bucket.
    func putBucketNotification(bucket: String, configuration: NotificationConfiguration) async throws
    /// Gets the notification configuration for a bucket.
    func getBucketNotification(bucket: String) async throws -> NotificationConfiguration?
    /// Deletes the notification configuration for a bucket.
    func deleteBucketNotification(bucket: String) async throws
    /// Publishes an event notification for the specified bucket and event.
    func publishEvent(bucket: String, event: S3EventType, key: String?, metadata: ObjectMetadata?, userIdentity: String?, sourceIPAddress: String?) async throws

    // VPC-Only Access
    /// Configures VPC-only access for a bucket.
    func putBucketVpcConfiguration(bucket: String, configuration: VpcConfiguration) async throws
    /// Gets the VPC configuration for a bucket.
    func getBucketVpcConfiguration(bucket: String) async throws -> VpcConfiguration?
    /// Deletes the VPC configuration for a bucket.
    func deleteBucketVpcConfiguration(bucket: String) async throws

    // Advanced Auditing
    /// Logs an audit event for compliance and security monitoring.
    func logAuditEvent(_ event: AuditEvent) async throws
    /// Retrieves audit events with optional filtering.
    func getAuditEvents(
        bucket: String?, principal: String?, eventType: AuditEventType?, startDate: Date?, endDate: Date?,
        limit: Int?, continuationToken: String?
    ) async throws -> (events: [AuditEvent], nextContinuationToken: String?)
    /// Deletes audit events older than the specified date.
    func deleteAuditEvents(olderThan: Date) async throws

    // Batch Operations
    /// Creates a new batch job for large-scale operations on objects.
    func createBatchJob(job: BatchJob) async throws -> String
    /// Retrieves information about a batch job.
    func getBatchJob(jobId: String) async throws -> BatchJob?
    /// Lists batch jobs with optional filtering.
    func listBatchJobs(bucket: String?, status: BatchJobStatus?, limit: Int?, continuationToken: String?) async throws -> (jobs: [BatchJob], nextContinuationToken: String?)
    /// Updates the status of a batch job.
    func updateBatchJobStatus(jobId: String, status: BatchJobStatus, message: String?) async throws
    /// Deletes a completed or failed batch job.
    func deleteBatchJob(jobId: String) async throws
    /// Executes a batch operation on a single object (called by the batch job processor).
    func executeBatchOperation(jobId: String, bucket: String, key: String) async throws
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

/// Configuration for object lock on a bucket.
public struct ObjectLockConfiguration: Codable, Sendable {
    /// Whether object lock is enabled for the bucket.
    public enum ObjectLockEnabled: String, Codable, Sendable {
        case enabled = "Enabled"
    }

    /// Default retention mode for objects in the bucket.
    public let objectLockEnabled: ObjectLockEnabled?
    /// Default retention period for objects.
    public let defaultRetention: DefaultRetention?

    public init(objectLockEnabled: ObjectLockEnabled? = nil, defaultRetention: DefaultRetention? = nil) {
        self.objectLockEnabled = objectLockEnabled
        self.defaultRetention = defaultRetention
    }

    /// Default retention configuration for bucket objects.
    public struct DefaultRetention: Codable, Sendable {
        public let mode: ObjectLockMode
        public let days: Int?
        public let years: Int?

        public init(mode: ObjectLockMode, days: Int? = nil, years: Int? = nil) {
            self.mode = mode
            self.days = days
            self.years = years
        }
    }
}

/// Result of data integrity verification.
public struct DataIntegrityResult: Sendable {
    /// Whether the data integrity check passed.
    public let isValid: Bool
    /// The checksum algorithm used.
    public let algorithm: ChecksumAlgorithm?
    /// The computed checksum value.
    public let computedChecksum: String?
    /// The stored checksum value.
    public let storedChecksum: String?
    /// Whether bitrot was detected.
    public let bitrotDetected: Bool
    /// Whether the data can be repaired.
    public let canRepair: Bool

    public init(
        isValid: Bool,
        algorithm: ChecksumAlgorithm? = nil,
        computedChecksum: String? = nil,
        storedChecksum: String? = nil,
        bitrotDetected: Bool = false,
        canRepair: Bool = false
    ) {
        self.isValid = isValid
        self.algorithm = algorithm
        self.computedChecksum = computedChecksum
        self.storedChecksum = storedChecksum
        self.bitrotDetected = bitrotDetected
        self.canRepair = canRepair
    }
}
