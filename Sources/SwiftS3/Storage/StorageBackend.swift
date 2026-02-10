import Foundation
import Hummingbird

struct ObjectMetadata: Sendable {
    let key: String
    let size: Int64
    let lastModified: Date
    let eTag: String?
    let contentType: String?
    let customMetadata: [String: String]

    init(
        key: String, size: Int64, lastModified: Date, eTag: String?, contentType: String? = nil,
        customMetadata: [String: String] = [:]
    ) {
        self.key = key
        self.size = size
        self.lastModified = lastModified
        self.eTag = eTag
        self.contentType = contentType
        self.customMetadata = customMetadata
    }
}

struct PartInfo: Sendable, Codable {
    let partNumber: Int
    let eTag: String
}

protocol StorageBackend: Sendable {
    func listBuckets() async throws -> [(name: String, created: Date)]
    func createBucket(name: String) async throws
    func deleteBucket(name: String) async throws

    func listObjects(bucket: String) async throws -> [ObjectMetadata]
    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: consuming Stream, size: Int64?,
        metadata: [String: String]?
    ) async throws -> String where Stream.Element == ByteBuffer  // Returns ETag
    func getObject(bucket: String, key: String, range: ValidatedRange?) async throws -> (
        metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?
    )
    func deleteObject(bucket: String, key: String) async throws
    func getObjectMetadata(bucket: String, key: String) async throws -> ObjectMetadata

    // Multipart Upload
    func createMultipartUpload(bucket: String, key: String, metadata: [String: String]?)
        async throws -> String
    func uploadPart<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, uploadId: String, partNumber: Int, data: consuming Stream,
        size: Int64?
    ) async throws -> String where Stream.Element == ByteBuffer
    func completeMultipartUpload(bucket: String, key: String, uploadId: String, parts: [PartInfo])
        async throws -> String
    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws
}

struct ValidatedRange: Sendable {
    let start: Int64
    let end: Int64
}
