import Hummingbird
import Foundation

struct ObjectMetadata: Sendable {
    let key: String
    let size: Int64
    let lastModified: Date
    let eTag: String?
}

protocol StorageBackend: Sendable {
    func listBuckets() async throws -> [(name: String, created: Date)]
    func createBucket(name: String) async throws
    func deleteBucket(name: String) async throws
    
    func listObjects(bucket: String) async throws -> [ObjectMetadata]
    func putObject(bucket: String, key: String, data: consuming some AsyncSequence<ByteBuffer, any Error> & Sendable, size: Int64?) async throws -> String // Returns ETag
    func getObject(bucket: String, key: String) async throws -> (metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?)
    func deleteObject(bucket: String, key: String) async throws
    func getObjectMetadata(bucket: String, key: String) async throws -> ObjectMetadata
}
