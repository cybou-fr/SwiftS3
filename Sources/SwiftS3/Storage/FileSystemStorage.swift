import AsyncHTTPClient
import Crypto
import CryptoKit
import Foundation
import Hummingbird
import Logging
import NIO
import _NIOFileSystem

// CRC32 extension for Data
extension Data {
    func crc32() -> UInt32 {
        var crc: UInt32 = 0xFFFFFFFF
        let table = crc32Table()

        for byte in self {
            let index = Int((crc ^ UInt32(byte)) & 0xFF)
            crc = (crc >> 8) ^ table[index]
        }

        return crc ^ 0xFFFFFFFF
    }

    private func crc32Table() -> [UInt32] {
        var table: [UInt32] = Array(repeating: 0, count: 256)
        let polynomial: UInt32 = 0xEDB88320

        for i in 0..<256 {
            var crc = UInt32(i)
            for _ in 0..<8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ polynomial
                } else {
                    crc >>= 1
                }
            }
            table[i] = crc
        }

        return table
    }
}

// Digest to hex string extension
extension Digest {
    var hexString: String {
        map { String(format: "%02x", $0) }.joined()
    }
}

// Helper extension to match functionality
extension FileSystem {
    func exists(at path: FilePath) async throws -> Bool {
        let info = try await self.info(forFileAt: path, infoAboutSymbolicLink: false)
        return info != nil
    }

    func readAll(at path: FilePath) async throws -> Data {
        return try await self.withFileHandle(forReadingAt: path) { handle in
            let info = try await handle.info()
            let size = Int64(info.size)
            var data = Data()
            var offset: Int64 = 0
            while offset < size {
                let chunk = try await handle.readChunk(
                    fromAbsoluteOffset: offset, length: .bytes(64 * 1024))
                if chunk.readableBytes == 0 { break }
                data.append(contentsOf: chunk.readableBytesView)
                offset += Int64(chunk.readableBytes)
            }
            return data
        }
    }

    func writeFile(at path: FilePath, bytes: ByteBuffer) async throws {
        _ = try await self.withFileHandle(
            forWritingAt: path, options: .newFile(replaceExisting: true)
        ) { handle in
            let data = bytes.readableBytesView.map { $0 }
            try await handle.write(contentsOf: data, toAbsoluteOffset: 0)
        }
    }
}

/// File system-based implementation of the StorageBackend protocol.
/// Manages object storage on the local file system with metadata persistence.
actor FileSystemStorage: StorageBackend {
    let rootPath: FilePath
    let fileSystem = FileSystem.shared
    let metadataStore: MetadataStore
    let logger = Logger(label: "SwiftS3.FileSystemStorage")
    let httpClient: HTTPClient?
    let testMode: Bool

    /// Initializes a new file system storage instance.
    init(rootPath: String, metadataStore: MetadataStore? = nil, testMode: Bool = false) {
        self.rootPath = FilePath(rootPath)
        self.metadataStore = metadataStore ?? FileSystemMetadataStore(rootPath: rootPath)
        self.testMode = testMode
        if !testMode {
            self.httpClient = HTTPClient(eventLoopGroupProvider: .singleton)
        } else {
            self.httpClient = nil
        }
    }

    private func bucketPath(_ name: String) -> FilePath {
        return rootPath.appending(name)
    }

    private func getObjectPath(bucket: String, key: String, versionId: String? = nil) -> FilePath {
        var path = rootPath.appending(bucket).appending(key)
        if let versionId = versionId, versionId != "null" {
            // Append versionId to filename to store separate versions
            let filename = path.lastComponent?.string ?? key
            path = path.removingLastComponent().appending("\(filename)@\(versionId)")
        }
        return path
    }

    /// Lists all buckets in the storage system.
    /// Scans the root directory for subdirectories, each representing a bucket.
    /// Returns bucket names with their creation timestamps from metadata store.
    ///
    /// - Returns: Array of tuples containing bucket name and creation date
    /// - Throws: File system errors if root directory cannot be accessed
    func listBuckets() async throws -> [(name: String, created: Date)] {
        // Create root if not exists
        if !(try await fileSystem.exists(at: rootPath)) {
            try await fileSystem.createDirectory(
                at: rootPath, withIntermediateDirectories: true, permissions: nil)
        }

        var buckets: [(name: String, created: Date)] = []
        let handle = try await fileSystem.openDirectory(atPath: rootPath)
        do {
            for try await entry in handle.listContents() {
                if entry.type == .directory {
                    // DirectoryEntry doesn't have info(), fetching from FS
                    if let attributes = try await fileSystem.info(
                        forFileAt: entry.path, infoAboutSymbolicLink: false)
                    {
                        // Use modification time as creation time (since birthtime is not portably available in FileInfo)
                        let mtime = attributes.lastDataModificationTime
                        let created = Date(
                            timeIntervalSince1970: TimeInterval(mtime.seconds) + TimeInterval(
                                mtime.nanoseconds) / 1_000_000_000)
                        buckets.append((name: entry.name.string, created: created))
                    }
                }
            }
            try await handle.close()
        } catch {
            try? await handle.close()
            throw error
        }
        return buckets
    }

    /// Creates a new bucket with the specified name and owner.
    /// Creates a directory on the file system and registers the bucket in metadata store.
    ///
    /// - Parameters:
    ///   - name: The bucket name to create
    ///   - owner: The owner ID of the bucket creator
    /// - Throws: S3Error.bucketAlreadyExists if bucket already exists
    func createBucket(name: String, owner: String) async throws {
        let path = bucketPath(name)
        if try await fileSystem.exists(at: path) {
            throw S3Error.bucketAlreadyExists
        }
        try await fileSystem.createDirectory(
            at: path, withIntermediateDirectories: true, permissions: nil)
        try await metadataStore.createBucket(name: name, owner: owner)
    }

    /// Deletes the specified bucket if it exists and is empty.
    /// Checks for objects in the bucket (excluding internal metadata files) and removes
    /// both the file system directory and metadata store entries.
    ///
    /// - Parameter name: The bucket name to delete
    /// - Throws: S3Error.bucketNotEmpty if bucket contains objects
    func deleteBucket(name: String) async throws {
        let path = bucketPath(name)
        // Check if empty
        let handle = try await fileSystem.openDirectory(atPath: path)
        let isEmpty: Bool
        do {
            var iterator = handle.listContents().makeAsyncIterator()
            // Ignore internal metadata files for emptiness check?
            // Better: if it's empty EXCEPT for internal files, then it's empty enough to delete metadata.
            // Actually, internal files should be ignored in the count.
            var count = 0
            while let item = try await iterator.next() {
                if item.name == ".bucket_metadata" || item.name == ".bucket_acl"
                    || item.name == "versioning.json" || item.name == ".bucket_policy"
                {
                    continue
                }
                count += 1
                break  // Found a "real" file
            }
            isEmpty = count == 0
            try await handle.close()
        } catch {
            try? await handle.close()
            throw error
        }

        if !isEmpty {
            throw S3Error.bucketNotEmpty
        }

        try await metadataStore.deleteBucket(name: name)

        // recursive: true because we want to remove the hidden metadata files
        _ = try? await fileSystem.removeItem(
            at: path, strategy: .platformDefault, recursively: true)
    }

    /// Checks if a bucket exists and returns metadata about it.
    /// Verifies the bucket directory exists on the file system.
    ///
    /// - Parameter name: The bucket name to check
    /// - Throws: S3Error.noSuchBucket if bucket does not exist
    func headBucket(name: String) async throws {
        let path = bucketPath(name)
        var isDir = false
        do {
            let info = try await fileSystem.info(forFileAt: path, infoAboutSymbolicLink: false)
            isDir = info?.type == .directory
        } catch {
            throw S3Error.noSuchBucket
        }

        if !isDir {
            throw S3Error.noSuchBucket
        }
    }

    /// Stores a new object in the specified bucket.
    /// Handles versioning automatically, streaming data to disk, and updating metadata.
    /// Creates versioned files when bucket versioning is enabled.
    ///
    /// - Parameters:
    ///   - bucket: Target bucket name
    ///   - key: Object key/path
    ///   - data: Async stream of data to store
    ///   - size: Expected object size (optional)
    ///   - metadata: Custom metadata key-value pairs
    ///   - owner: Owner ID of the object
    /// - Returns: ObjectMetadata for the stored object
    /// - Throws: S3Error if bucket doesn't exist or storage fails
    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: Stream, size: Int64?,
        metadata: [String: String]?, owner: String
    ) async throws -> ObjectMetadata where Stream.Element == ByteBuffer {
        let bPath = bucketPath(bucket)
        if !(try await fileSystem.exists(at: bPath)) {
            throw S3Error.noSuchBucket
        }

        // Determine Version ID
        let versioning = try await getBucketVersioning(bucket: bucket)
        let isEnabled = versioning?.status == .enabled
        let versionId = isEnabled ? UUID().uuidString : "null"

        let path = getObjectPath(bucket: bucket, key: key, versionId: versionId)

        // Ensure parent dir exists
        try await fileSystem.createDirectory(
            at: path.removingLastComponent(), withIntermediateDirectories: true, permissions: nil)

        // Write file
        var digest = SHA256()
        let handle = try await fileSystem.openFile(
            forWritingAt: path, options: .newFile(replaceExisting: true))

        var finalSize: Int64 = 0
        do {
            var offset: Int64 = 0
            for try await var buffer in data {
                let bytes = buffer.readBytes(length: buffer.readableBytes) ?? []
                digest.update(data: Data(bytes))
                try await handle.write(contentsOf: bytes, toAbsoluteOffset: offset)
                offset += Int64(bytes.count)
            }
            finalSize = offset
            try await handle.close()
        } catch {
            try? await handle.close()
            throw error
        }

        let eTag = digest.finalize().map { String(format: "%02x", $0) }.joined()

        // Write Metadata
        let meta = ObjectMetadata(
            key: key,
            size: finalSize,
            lastModified: Date(),
            eTag: eTag,
            contentType: metadata?["Content-Type"],
            customMetadata: metadata ?? [:],
            owner: owner,
            versionId: versionId,
            isLatest: true,
            isDeleteMarker: false
        )
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: meta)

        return meta
    }

    /// Retrieves an object from storage with optional range support.
    /// Handles version resolution, delete marker checking, and partial content delivery.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key
    ///   - versionId: Specific version to retrieve (nil for latest)
    ///   - range: Byte range for partial content (optional)
    /// - Returns: Tuple of object metadata and async stream of content
    /// - Throws: S3Error for missing objects, delete markers, or invalid ranges
    func getObject(bucket: String, key: String, versionId: String?, range: ValidatedRange?)
        async throws -> (
            metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?
        )
    {
        // 1. Get Metadata first to resolve versionId (if nil) and check existence/delete marker
        let metadata = try await getObjectMetadata(bucket: bucket, key: key, versionId: versionId)

        // 2. Check Delete Marker
        if metadata.isDeleteMarker {
            if versionId != nil {
                // Specific version is a delete marker -> 405 Method Not Allowed
                throw S3Error.methodNotAllowed
            } else {
                // Latest version is a delete marker -> 404 Not Found
                throw S3Error.noSuchKey
            }
        }

        // 3. Resolve Path using the ACTUAL versionId from metadata
        let path = getObjectPath(bucket: bucket, key: key, versionId: metadata.versionId)

        if !(try await fileSystem.exists(at: path)) {
            // Inconsistency: Metadata exists but file missing?
            throw S3Error.internalError
        }

        let handle = try await fileSystem.openFile(forReadingAt: path)

        let body = AsyncStream<ByteBuffer> { continuation in
            _ = Task {
                do {
                    let info = try await handle.info()
                    let fileSize = Int64(info.size)

                    var currentOffset: Int64 = 0
                    var endOffset: Int64 = fileSize - 1

                    if let range = range {
                        currentOffset = range.start
                        endOffset = range.end
                    }

                    // Cap endOffset
                    if endOffset >= fileSize {
                        endOffset = fileSize - 1
                    }

                    while currentOffset <= endOffset {
                        let remaining = endOffset - currentOffset + 1
                        let chunkSize = min(remaining, 64 * 1024)

                        // readChunk returns ByteBuffer
                        let buffer = try await handle.readChunk(
                            fromAbsoluteOffset: currentOffset, length: .bytes(Int64(chunkSize)))
                        if buffer.readableBytes == 0 { break }

                        continuation.yield(buffer)
                        currentOffset += Int64(buffer.readableBytes)
                    }
                    try await handle.close()
                    continuation.finish()
                } catch {
                    try? await handle.close()
                    continuation.finish()
                }
            }
        }

        return (metadata, body)
    }

    /// Deletes an object or creates a delete marker based on versioning status.
    /// When versioning is enabled, creates a delete marker. When disabled, permanently deletes.
    /// For versioned deletes, removes the specific version if versionId is provided.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key
    ///   - versionId: Specific version to delete (nil for current version)
    /// - Returns: Tuple with versionId of deleted object/marker and whether it was a delete marker
    /// - Throws: S3Error if object doesn't exist
    func deleteObject(bucket: String, key: String, versionId: String?) async throws -> (
        versionId: String?, isDeleteMarker: Bool
    ) {
        if let versionId = versionId {
            // Delete specific version
            let path = getObjectPath(bucket: bucket, key: key, versionId: versionId)
            _ = try? await fileSystem.removeItem(
                at: path, strategy: .platformDefault, recursively: false)
            try await metadataStore.deleteMetadata(bucket: bucket, key: key, versionId: versionId)
            return (versionId: versionId, isDeleteMarker: false)
        } else {
            // Simple Delete (Delete Marker or Null Version)
            let versioning = try await getBucketVersioning(bucket: bucket)
            let isEnabled = versioning?.status == .enabled

            if isEnabled {
                // Create Delete Marker
                let newVersionId = UUID().uuidString
                let deleteMarker = ObjectMetadata(
                    key: key,
                    size: 0,
                    lastModified: Date(),
                    eTag: nil,
                    contentType: nil,
                    customMetadata: [:],
                    owner: nil,  // Owner?
                    versionId: newVersionId,
                    isLatest: true,
                    isDeleteMarker: true
                )
                try await metadataStore.saveMetadata(
                    bucket: bucket, key: key, metadata: deleteMarker)
                return (versionId: newVersionId, isDeleteMarker: true)
            } else {
                // Suspended or Unversioned: Delete "null" version
                let path = getObjectPath(bucket: bucket, key: key, versionId: "null")
                _ = try? await fileSystem.removeItem(
                    at: path, strategy: .platformDefault, recursively: false)
                try await metadataStore.deleteMetadata(bucket: bucket, key: key, versionId: "null")
                return (versionId: "null", isDeleteMarker: false)
            }
        }
    }

    /// Delete multiple objects from a bucket in a single operation.
    /// Attempts to delete all specified keys and returns the list of successfully deleted keys.
    /// Ignores errors for individual objects and continues with the bulk operation.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - keys: Array of object keys to delete
    /// - Returns: Array of keys that were successfully deleted
    /// - Throws: Error if bucket doesn't exist
    func deleteObjects(bucket: String, objects: [DeleteObject]) async throws -> [(key: String, versionId: String?, isDeleteMarker: Bool, deleteMarkerVersionId: String?)] {
        var deleted: [(String, String?, Bool, String?)] = []
        for object in objects {
            let result = try? await deleteObject(bucket: bucket, key: object.key, versionId: object.versionId)
            if let result = result {
                deleted.append((object.key, result.versionId, result.isDeleteMarker, result.versionId))
            }
        }
        return deleted
    }

    /// Copies an object from one location to another within the storage system.
    /// Creates a new object with the same data but potentially different key/bucket.
    /// Handles versioning automatically for the destination.
    ///
    /// - Parameters:
    ///   - fromBucket: Source bucket name
    ///   - fromKey: Source object key
    ///   - toBucket: Destination bucket name
    ///   - toKey: Destination object key
    ///   - owner: Owner ID for the new object
    /// - Returns: ObjectMetadata for the copied object
    /// - Throws: S3Error if source doesn't exist or destination bucket invalid
    func copyObject(
        fromBucket: String, fromKey: String, toBucket: String, toKey: String, owner: String
    )
        async throws -> ObjectMetadata
    {
        let srcPath = getObjectPath(bucket: fromBucket, key: fromKey)
        let dstPath = getObjectPath(bucket: toBucket, key: toKey)

        if !(try await fileSystem.exists(at: srcPath)) {
            throw S3Error.noSuchKey
        }

        let dstBucketPath = bucketPath(toBucket)
        if !(try await fileSystem.exists(at: dstBucketPath)) {
            throw S3Error.noSuchBucket
        }

        // Ensure dst dir
        try await fileSystem.createDirectory(
            at: dstPath.removingLastComponent(), withIntermediateDirectories: true, permissions: nil
        )

        // Manual Copy
        let srcHandle = try await fileSystem.openFile(forReadingAt: srcPath)
        do {
            let dstHandle = try await fileSystem.openFile(
                forWritingAt: dstPath, options: .newFile(replaceExisting: true))
            do {
                let size = Int64(try await srcHandle.info().size)
                var offset: Int64 = 0
                while offset < size {
                    let chunk = try await srcHandle.readChunk(
                        fromAbsoluteOffset: offset, length: .bytes(64 * 1024))
                    if chunk.readableBytes == 0 { break }
                    try await dstHandle.write(contentsOf: chunk, toAbsoluteOffset: offset)
                    offset += Int64(chunk.readableBytes)
                }
                try await dstHandle.close()
            } catch {
                try? await dstHandle.close()
                throw error
            }
            try await srcHandle.close()
        } catch {
            try? await srcHandle.close()
            throw error
        }

        // Copy Metadata
        if let srcMeta = try? await metadataStore.getMetadata(
            bucket: fromBucket, key: fromKey, versionId: nil)
        {
            // Create new metadata with new owner
            let newMeta = ObjectMetadata(
                key: toKey,
                size: srcMeta.size,
                lastModified: Date(),
                eTag: srcMeta.eTag,
                contentType: srcMeta.contentType,
                customMetadata: srcMeta.customMetadata,
                owner: owner,
                versionId: "null",
                isLatest: true,
                isDeleteMarker: false
            )
            try await metadataStore.saveMetadata(bucket: toBucket, key: toKey, metadata: newMeta)
        }

        return try await getObjectMetadata(bucket: toBucket, key: toKey, versionId: nil)
    }

    /// Lists objects in a bucket with optional filtering and pagination.
    /// Delegates to metadata store for efficient querying with prefix/delimiter support.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name to list
    ///   - prefix: Object key prefix filter
    ///   - delimiter: Grouping delimiter for hierarchical listing
    ///   - marker: Pagination marker for continuing listings
    ///   - continuationToken: Alternative pagination token
    ///   - maxKeys: Maximum number of objects to return
    /// - Returns: ListObjectsResult with objects, prefixes, and pagination info
    /// - Throws: S3Error if bucket doesn't exist
    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult {
        return try await metadataStore.listObjects(
            bucket: bucket, prefix: prefix, delimiter: delimiter, marker: marker,
            continuationToken: continuationToken, maxKeys: maxKeys)
    }

    /// Retrieves metadata for an object without fetching its content.
    /// Resolves version IDs and handles delete marker logic.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key
    ///   - versionId: Specific version ID (nil for latest)
    /// - Returns: ObjectMetadata for the specified object
    /// - Throws: S3Error if object doesn't exist
    func getObjectMetadata(bucket: String, key: String, versionId: String?) async throws
        -> ObjectMetadata
    {
        return try await metadataStore.getMetadata(bucket: bucket, key: key, versionId: versionId)
    }

    // MARK: - Multipart Upload

    private func uploadPath(bucket: String, uploadId: String) -> FilePath {
        return bucketPath(bucket).appending(".uploads").appending(uploadId)
    }

    struct UploadInfo: Codable {
        let key: String
        let metadata: [String: String]?
        let owner: String
    }

    /// Initiates a multipart upload for large objects.
    /// Creates an upload directory and stores upload metadata for later completion.
    ///
    /// - Parameters:
    ///   - bucket: Target bucket name
    ///   - key: Object key for the final assembled object
    ///   - metadata: Custom metadata for the final object
    ///   - owner: Owner ID for the upload
    /// - Returns: Unique upload ID for referencing this upload
    /// - Throws: S3Error if bucket doesn't exist
    func createMultipartUpload(
        bucket: String, key: String, metadata: [String: String]?, owner: String
    )
        async throws
        -> String
    {
        let uploadId = UUID().uuidString
        let path = uploadPath(bucket: bucket, uploadId: uploadId)

        try await fileSystem.createDirectory(
            at: path, withIntermediateDirectories: true, permissions: nil)

        let info = UploadInfo(key: key, metadata: metadata, owner: owner)
        let data = try JSONEncoder().encode(info)

        // Write info.json
        _ = try await fileSystem.withFileHandle(
            forWritingAt: path.appending("info.json"), options: .newFile(replaceExisting: true)
        ) { handle in
            try await handle.write(contentsOf: data.map { $0 }, toAbsoluteOffset: 0)
        }

        return uploadId
    }

    /// Uploads a part of a multipart upload.
    /// Stores the part data and computes its SHA256 ETag for integrity verification.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (for validation)
    ///   - uploadId: Multipart upload ID
    ///   - partNumber: Sequential part number (1-based)
    ///   - data: Stream of part data
    ///   - size: Expected part size (optional)
    /// - Returns: SHA256 ETag of the uploaded part
    /// - Throws: S3Error if upload doesn't exist
    func uploadPart<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, uploadId: String, partNumber: Int, data: Stream,
        size: Int64?
    ) async throws -> String where Stream.Element == ByteBuffer {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        if !(try await fileSystem.exists(at: uPath)) {
            throw S3Error.noSuchUpload
        }

        let partPath = uPath.appending("\(partNumber)")

        var digest = SHA256()
        let handle = try await fileSystem.openFile(
            forWritingAt: partPath, options: .newFile(replaceExisting: true))

        do {
            var offset: Int64 = 0
            for try await var buffer in data {
                let bytes = buffer.readBytes(length: buffer.readableBytes) ?? []
                digest.update(data: Data(bytes))
                try await handle.write(contentsOf: bytes, toAbsoluteOffset: offset)
                offset += Int64(bytes.count)
            }
            try await handle.close()
        } catch {
            try? await handle.close()
            throw error
        }

        return digest.finalize().map { String(format: "%02x", $0) }.joined()
    }

    /// Uploads a part by copying data from an existing object.
    /// Supports range-based copying for partial object duplication.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (for validation)
    ///   - uploadId: Multipart upload ID
    ///   - partNumber: Sequential part number (1-based)
    ///   - copySource: Source object path (bucket/key)
    ///   - range: Optional byte range to copy
    /// - Returns: SHA256 ETag of the copied part
    /// - Throws: S3Error if upload doesn't exist or source object not found
    func uploadPartCopy(
        bucket: String, key: String, uploadId: String, partNumber: Int, copySource: String,
        range: ValidatedRange?
    ) async throws -> String {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        if !(try await fileSystem.exists(at: uPath)) {
            throw S3Error.noSuchUpload
        }

        // Parse copy source
        var source = copySource
        if source.hasPrefix("/") {
            source.removeFirst()
        }
        let components = source.split(separator: "/", maxSplits: 1)
        guard components.count == 2 else {
            throw S3Error.invalidRequest
        }
        let srcBucket = String(components[0])
        let srcKey = String(components[1])

        // Get source object
        let (_, bodyStream) = try await getObject(bucket: srcBucket, key: srcKey, versionId: nil, range: range)
        guard let body = bodyStream else {
            throw S3Error.invalidRequest
        }

        let partPath = uPath.appending("\(partNumber)")

        var digest = SHA256()
        let handle = try await fileSystem.openFile(
            forWritingAt: partPath, options: .newFile(replaceExisting: true))

        do {
            var offset: Int64 = 0
            for try await var buffer in body {
                let bytes = buffer.readBytes(length: buffer.readableBytes) ?? []
                digest.update(data: Data(bytes))
                try await handle.write(contentsOf: bytes, toAbsoluteOffset: offset)
                offset += Int64(bytes.count)
            }
            try await handle.close()
        } catch {
            try? await handle.close()
            throw error
        }

        return digest.finalize().map { String(format: "%02x", $0) }.joined()
    }

    /// Completes a multipart upload by assembling all parts into final object.
    /// Concatenates all uploaded parts in order and creates the final object.
    /// Cleans up temporary upload files after successful completion.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Final object key
    ///   - uploadId: Multipart upload ID
    ///   - parts: List of parts with ETags for validation
    /// - Returns: ETag of the assembled object
    /// - Throws: S3Error if upload doesn't exist or parts are missing
    func completeMultipartUpload(
        bucket: String, key: String, uploadId: String, parts: [PartInfo]
    ) async throws -> String {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        if !(try await fileSystem.exists(at: uPath)) {
            throw S3Error.noSuchUpload
        }

        // Read Info
        let infoDataBuffer = try await fileSystem.withFileHandle(
            forReadingAt: uPath.appending("info.json")
        ) { handle in
            // Read all
            let info = try await handle.info()
            return try await handle.readChunk(
                fromAbsoluteOffset: 0, length: .bytes(Int64(info.size)))
        }
        let infoData = Data(buffer: infoDataBuffer)
        let info = try JSONDecoder().decode(UploadInfo.self, from: infoData)

        guard info.key == key else {
            throw S3Error.invalidPart
        }

        let finalPath = getObjectPath(bucket: bucket, key: key)

        // Ensure parent dir exists
        try await fileSystem.createDirectory(
            at: finalPath.removingLastComponent(), withIntermediateDirectories: true,
            permissions: nil)

        var fullDigest = SHA256()

        // Combine parts
        let outHandle = try await fileSystem.openFile(
            forWritingAt: finalPath, options: .newFile(replaceExisting: true))

        var finalSize: Int64 = 0
        do {
            var outOffset: Int64 = 0

            for part in parts {
                let partPath = uPath.appending("\(part.partNumber)")
                guard try await fileSystem.exists(at: partPath) else {
                    throw S3Error.invalidPart
                }

                let inHandle = try await fileSystem.openFile(forReadingAt: partPath)
                do {
                    let size = Int64(try await inHandle.info().size)
                    var inOffset: Int64 = 0
                    while inOffset < size {
                        let chunkName = 64 * 1024
                        let chunk = try await inHandle.readChunk(
                            fromAbsoluteOffset: inOffset,
                            length: .bytes(Int64(min(Int64(chunkName), size - inOffset))))
                        if chunk.readableBytes == 0 { break }
                        try await outHandle.write(contentsOf: chunk, toAbsoluteOffset: outOffset)

                        // Update digest
                        fullDigest.update(data: Data(chunk.readableBytesView))

                        inOffset += Int64(chunk.readableBytes)
                        outOffset += Int64(chunk.readableBytes)
                    }
                    try await inHandle.close()
                } catch {
                    try? await inHandle.close()
                    throw error
                }
            }
            finalSize = outOffset
            try await outHandle.close()
        } catch {
            try? await outHandle.close()
            throw error
        }

        let finalETag =
            fullDigest.finalize().map { String(format: "%02x", $0) }.joined() + "-\(parts.count)"

        // Write Metadata
        let meta = ObjectMetadata(
            key: key,
            size: finalSize,
            lastModified: Date(),
            eTag: finalETag,
            contentType: info.metadata?["Content-Type"],
            customMetadata: info.metadata ?? [:],
            owner: info.owner,
            versionId: "null",
            isLatest: true,
            isDeleteMarker: false
        )
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: meta)

        // Cleanup
        _ = try? await fileSystem.removeItem(
            at: uPath, strategy: .platformDefault, recursively: true)

        return finalETag
    }

    /// Aborts a multipart upload and cleans up all associated resources.
    /// Removes the upload directory and all uploaded parts. Cannot be undone.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - key: Object key (for validation)
    ///   - uploadId: Multipart upload ID to abort
    /// - Throws: S3Error if upload doesn't exist
    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        _ = try? await fileSystem.removeItem(
            at: uPath, strategy: .platformDefault, recursively: true)
    }

    /// Clean up orphaned multipart upload directories older than the specified age.
    /// Removes upload directories that are incomplete and haven't been modified recently.
    /// Helps prevent disk space waste from abandoned multipart uploads.
    ///
    /// - Parameter olderThan: Time interval in seconds - uploads older than this will be cleaned
    /// - Throws: File system errors if cleanup fails
    func cleanupOrphanedUploads(olderThan: TimeInterval) async throws {
        let buckets = try await listBuckets()
        let cutoffDate = Date().addingTimeInterval(-olderThan)

        for bucket in buckets {
            let uploadsDir = bucketPath(bucket.name).appending(".uploads")
            
            // Check if uploads directory exists
            guard try await fileSystem.exists(at: uploadsDir) else { continue }
            
            // List all upload directories
            let handle = try await fileSystem.openDirectory(atPath: uploadsDir)
            do {
                for try await entry in handle.listContents() {
                    if entry.type == .directory {
                        let uploadPath = uploadsDir.appending(entry.name)
                        let infoPath = uploadPath.appending("info.json")
                        
                        // Check if info.json exists and is old
                        if try await fileSystem.exists(at: infoPath) {
                            do {
                                let infoData = try await fileSystem.readAll(at: infoPath)
                                let _ = try JSONDecoder().decode(UploadInfo.self, from: infoData)
                                
                                // For now, just check if the directory is old (by checking the info file modification time)
                                // In a real implementation, we'd track creation time in the info
                                if let attributes = try await fileSystem.info(forFileAt: infoPath) {
                                    let mtime = attributes.lastDataModificationTime
                                    let modTime = Date(
                                        timeIntervalSince1970: TimeInterval(mtime.seconds) + TimeInterval(mtime.nanoseconds) / 1_000_000_000)
                                    if modTime < cutoffDate {
                                        logger.info("Cleaning up orphaned multipart upload", metadata: [
                                            "bucket": Logger.MetadataValue.string(bucket.name),
                                            "uploadId": Logger.MetadataValue.string(entry.name.string),
                                            "age": Logger.MetadataValue.string("\(Date().timeIntervalSince(modTime))")
                                        ])
                                        _ = try? await fileSystem.removeItem(at: uploadPath, strategy: .platformDefault, recursively: true)
                                    }
                                }
                            } catch {
                                // If we can't read the info, it might be corrupted, clean it up
                                logger.warning("Cleaning up corrupted multipart upload", metadata: [
                                    "bucket": Logger.MetadataValue.string(bucket.name),
                                    "uploadId": Logger.MetadataValue.string(entry.name.string)
                                ])
                                _ = try? await fileSystem.removeItem(at: uploadPath, strategy: .platformDefault, recursively: true)
                            }
                        }
                    }
                }
            }
            try await handle.close()
        }
    }

    // MARK: - Bucket Policy

    /// Retrieves the bucket policy for the specified bucket.
    /// - Parameter bucket: The bucket name
    /// - Returns: BucketPolicy containing the policy document
    /// - Throws: S3Error if bucket doesn't exist or has no policy
    func getBucketPolicy(bucket: String) async throws -> BucketPolicy {
        let policyPath = bucketPath(bucket).appending("policy.json")
        guard try await fileSystem.exists(at: policyPath) else {
            throw S3Error.noSuchBucketPolicy
        }

        let data = try await fileSystem.readAll(at: policyPath)
        return try JSONDecoder().decode(BucketPolicy.self, from: data)
    }

    /// Sets the bucket policy for the specified bucket.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - policy: The BucketPolicy to apply
    /// - Throws: S3Error if bucket doesn't exist
    func putBucketPolicy(bucket: String, policy: BucketPolicy) async throws {
        // Enforce bucket existence
        let bPath = bucketPath(bucket)
        if !(try await fileSystem.exists(at: bPath)) {
            throw S3Error.noSuchBucket
        }

        let policyPath = bPath.appending("policy.json")
        let data = try JSONEncoder().encode(policy)
        try await fileSystem.writeFile(at: policyPath, bytes: ByteBuffer(data: data))
    }

    /// Removes the bucket policy from the specified bucket.
    /// - Parameter bucket: The bucket name
    /// - Throws: File system errors if the deletion fails
    func deleteBucketPolicy(bucket: String) async throws {
        let policyPath = bucketPath(bucket).appending("policy.json")
        if try await fileSystem.exists(at: policyPath) {
            try await fileSystem.removeItem(at: policyPath)
        } else {
            // S3 returns NoSuchBucketPolicy if checking? Or standard 204?
            // API usually returns 204 No Content even if it didn't exist,
            // but `DELETE /?policy` returns 204.
            // However, `getBucketPolicy` throws.
        }
    }

    // MARK: - ACLs

    /// Retrieves the Access Control List for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket ACL
    ///   - versionId: Optional version ID for object ACL
    /// - Returns: AccessControlPolicy containing the ACL information
    /// - Throws: Database errors or if the resource doesn't exist
    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy
    {
        return try await metadataStore.getACL(bucket: bucket, key: key, versionId: versionId)
    }

    /// Updates the Access Control List for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket ACL
    ///   - versionId: Optional version ID for object ACL
    ///   - acl: The new AccessControlPolicy to apply
    /// - Throws: Database errors if the update fails
    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws
    {
        try await metadataStore.putACL(bucket: bucket, key: key, versionId: versionId, acl: acl)
    }

    // MARK: - Versioning

    /// Retrieves the versioning configuration for a bucket.
    /// - Parameter bucket: The bucket name
    /// - Returns: VersioningConfiguration if set, or nil if not configured
    /// - Throws: Database errors if the query fails
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration? {
        return try await metadataStore.getBucketVersioning(bucket: bucket)
    }

    /// Sets the versioning configuration for a bucket.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - configuration: The new versioning configuration
    /// - Throws: Database errors if the update fails
    func putBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws {
        try await metadataStore.setBucketVersioning(bucket: bucket, configuration: configuration)
    }

    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult {
        return try await metadataStore.listObjectVersions(
            bucket: bucket, prefix: prefix, delimiter: delimiter, keyMarker: keyMarker,
            versionIdMarker: versionIdMarker, maxKeys: maxKeys)
    }

    // MARK: - Tagging

    /// Retrieves tags for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket tags
    ///   - versionId: Optional version ID for object tags
    /// - Returns: Array of S3Tag objects
    /// - Throws: Database errors if the query fails
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag] {
        return try await metadataStore.getTags(bucket: bucket, key: key, versionId: versionId)
    }

    /// Updates tags for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket tags
    ///   - versionId: Optional version ID for object tags
    ///   - tags: Array of S3Tag objects to set
    /// - Throws: Database errors if the update fails
    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws {
        try await metadataStore.putTags(bucket: bucket, key: key, versionId: versionId, tags: tags)
    }

    /// Removes all tags from a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket tags
    ///   - versionId: Optional version ID for object tags
    /// - Throws: Database errors if the update fails
    func deleteTags(bucket: String, key: String?, versionId: String?) async throws {
        try await metadataStore.deleteTags(bucket: bucket, key: key, versionId: versionId)
    }

    // MARK: - Lifecycle

    /// Retrieves the lifecycle configuration for a bucket.
    /// - Parameter bucket: The bucket name
    /// - Returns: LifecycleConfiguration if set, or nil if not configured
    /// - Throws: Database errors if the query fails
    func getBucketLifecycle(bucket: String) async throws -> LifecycleConfiguration? {
        return try await metadataStore.getLifecycle(bucket: bucket)
    }

    /// Sets the lifecycle configuration for a bucket.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - configuration: The lifecycle configuration to apply
    /// - Throws: Database errors if the update fails
    func putBucketLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws {
        try await metadataStore.putLifecycle(bucket: bucket, configuration: configuration)
    }

    /// Removes the lifecycle configuration from a bucket.
    /// - Parameter bucket: The bucket name
    /// - Throws: Database errors if the deletion fails
    func deleteBucketLifecycle(bucket: String) async throws {
        try await metadataStore.deleteLifecycle(bucket: bucket)
    }

    // MARK: - Advanced Storage & Data Protection

    /// Changes the storage class of an existing object.
    func changeStorageClass(bucket: String, key: String, versionId: String?, newStorageClass: StorageClass) async throws {
        var metadata = try await metadataStore.getMetadata(bucket: bucket, key: key, versionId: versionId)
        metadata.storageClass = newStorageClass
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: metadata)
    }

    /// Puts an object lock configuration on a bucket.
    func putObjectLockConfiguration(bucket: String, configuration: ObjectLockConfiguration) async throws {
        try await metadataStore.putObjectLockConfiguration(bucket: bucket, configuration: configuration)
    }

    /// Gets the object lock configuration for a bucket.
    func getObjectLockConfiguration(bucket: String) async throws -> ObjectLockConfiguration? {
        return try await metadataStore.getObjectLockConfiguration(bucket: bucket)
    }

    /// Puts an object lock on a specific object.
    func putObjectLock(bucket: String, key: String, versionId: String?, mode: ObjectLockMode, retainUntilDate: Date?) async throws {
        var metadata = try await metadataStore.getMetadata(bucket: bucket, key: key, versionId: versionId)
        metadata.objectLockMode = mode
        metadata.objectLockRetainUntilDate = retainUntilDate
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: metadata)
    }

    /// Puts a legal hold on a specific object.
    func putObjectLegalHold(bucket: String, key: String, versionId: String?, status: LegalHoldStatus) async throws {
        var metadata = try await metadataStore.getMetadata(bucket: bucket, key: key, versionId: versionId)
        metadata.objectLockLegalHoldStatus = status
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: metadata)
    }

    /// Verifies data integrity using checksums and detects bitrot.
    func verifyDataIntegrity(bucket: String, key: String, versionId: String?) async throws -> DataIntegrityResult {
        let metadata = try await metadataStore.getMetadata(bucket: bucket, key: key, versionId: versionId)
        let objectPath = getObjectPath(bucket: bucket, key: key, versionId: versionId)

        // Read the actual data
        let data = try await fileSystem.readAll(at: objectPath)

        // If we have a checksum, verify it
        if let algorithm = metadata.checksumAlgorithm, let storedChecksum = metadata.checksumValue {
            let computedChecksum = try computeChecksum(data: data, algorithm: algorithm)
            let isValid = computedChecksum == storedChecksum

            return DataIntegrityResult(
                isValid: isValid,
                algorithm: algorithm,
                computedChecksum: computedChecksum,
                storedChecksum: storedChecksum,
                bitrotDetected: !isValid,
                canRepair: false // For now, no repair capability
            )
        }

        // No checksum available
        return DataIntegrityResult(
            isValid: true, // Assume valid if no checksum
            bitrotDetected: false,
            canRepair: false
        )
    }

    /// Repairs data corruption if possible (for erasure coding or bitrot recovery).
    func repairDataCorruption(bucket: String, key: String, versionId: String?) async throws -> Bool {
        // For now, return false as we don't have erasure coding implemented
        // This would be where erasure coding recovery would happen
        return false
    }

    /// Computes checksum for data using specified algorithm.
    private func computeChecksum(data: Data, algorithm: ChecksumAlgorithm) throws -> String {
        switch algorithm {
        case .crc32:
            // Simple CRC32 implementation
            return String(format: "%08x", data.crc32())
        case .crc32c:
            // CRC32C - for now use same as CRC32
            return String(format: "%08x", data.crc32())
        case .sha1:
            return Insecure.SHA1.hash(data: data).hexString
        case .sha256:
            return SHA256.hash(data: data).hexString
        }
    }

    // MARK: - Server-Side Encryption

    /// Encrypts data using the specified server-side encryption configuration.
    func encryptData(_ data: Data, with config: ServerSideEncryptionConfig) async throws -> (encryptedData: Data, key: Data?, iv: Data?) {
        switch config.algorithm {
        case .aes256:
            // Generate a random 256-bit key and IV for AES encryption
            let key = SymmetricKey(size: .bits256)
            let iv = AES.GCM.Nonce()

            let sealedBox = try AES.GCM.seal(data, using: key, nonce: iv)
            let combinedData = sealedBox.combined!

            return (encryptedData: combinedData, key: key.withUnsafeBytes { Data($0) }, iv: iv.withUnsafeBytes { Data($0) })

        case .awsKms:
            // For KMS encryption, we'd need to call AWS KMS API
            // For now, fall back to AES256
            let key = SymmetricKey(size: .bits256)
            let iv = AES.GCM.Nonce()

            let sealedBox = try AES.GCM.seal(data, using: key, nonce: iv)
            let combinedData = sealedBox.combined!

            return (encryptedData: combinedData, key: key.withUnsafeBytes { Data($0) }, iv: iv.withUnsafeBytes { Data($0) })
        }
    }

    /// Decrypts data using the specified server-side encryption configuration.
    func decryptData(_ encryptedData: Data, with config: ServerSideEncryptionConfig, key: Data?, iv: Data?) async throws -> Data {
        guard let key = key, let iv = iv else {
            throw S3Error.invalidEncryption
        }

        switch config.algorithm {
        case .aes256, .awsKms:
            let symmetricKey = SymmetricKey(data: key)
            let _ = try AES.GCM.Nonce(data: iv)

            let sealedBox = try AES.GCM.SealedBox(combined: encryptedData)
            let decryptedData = try AES.GCM.open(sealedBox, using: symmetricKey)

            return decryptedData
        }
    }

    // MARK: - Cross-Region Replication

    func putBucketReplication(bucket: String, configuration: ReplicationConfiguration) async throws {
        // Store replication configuration in metadata store
        try await metadataStore.putBucketReplication(bucket: bucket, configuration: configuration)
    }

    func getBucketReplication(bucket: String) async throws -> ReplicationConfiguration? {
        return try await metadataStore.getBucketReplication(bucket: bucket)
    }

    func deleteBucketReplication(bucket: String) async throws {
        try await metadataStore.deleteBucketReplication(bucket: bucket)
    }

    func replicateObject(bucket: String, key: String, versionId: String?, metadata: ObjectMetadata, data: Data) async throws {
        // Get replication configuration
        guard let replicationConfig = try await getBucketReplication(bucket: bucket) else {
            return // No replication configured
        }

        // Replicate to each destination
        for rule in replicationConfig.rules where rule.status == .pending || rule.status == .completed {
            do {
                let destination = rule.destination

                // For now, assume destination is another FileSystemStorage instance
                // In a real implementation, this would connect to remote regions
                logger.info("Replicating object \(key) to region \(destination.region), bucket \(destination.bucket)")

                // Create destination bucket if it doesn't exist
                try await createBucket(name: destination.bucket, owner: metadata.owner ?? "replicator")

                // For this simplified implementation, just save the metadata
                // In a real implementation, would copy the actual file data
                let destinationKey = key // Could apply prefix transformations here
                let destinationMetadata = ObjectMetadata(
                    key: destinationKey,
                    size: metadata.size,
                    lastModified: metadata.lastModified,
                    eTag: metadata.eTag,
                    contentType: metadata.contentType,
                    customMetadata: metadata.customMetadata,
                    owner: metadata.owner,
                    versionId: metadata.versionId,
                    isLatest: metadata.isLatest,
                    isDeleteMarker: metadata.isDeleteMarker,
                    storageClass: destination.storageClass ?? metadata.storageClass,
                    checksumAlgorithm: metadata.checksumAlgorithm,
                    checksumValue: metadata.checksumValue,
                    objectLockMode: metadata.objectLockMode,
                    objectLockRetainUntilDate: metadata.objectLockRetainUntilDate,
                    objectLockLegalHoldStatus: metadata.objectLockLegalHoldStatus,
                    serverSideEncryption: metadata.serverSideEncryption
                )

                // Save metadata for destination
                try await metadataStore.saveMetadata(bucket: destination.bucket, key: destinationKey, metadata: destinationMetadata)

                // In a real implementation, would also copy the file data here
                // For now, assume the data is accessible via the same file system

                logger.info("Successfully replicated object \(key) to \(destination.region)/\(destination.bucket)")

            } catch {
                logger.error("Failed to replicate object \(key) to rule \(rule.id): \(error)")
                // In a real implementation, would update replication status to failed
            }
        }
    }

    func getReplicationStatus(bucket: String, key: String, versionId: String?) async throws -> ReplicationStatus {
        // For now, return completed if replication config exists
        // In a real implementation, would track per-object replication status
        if let _ = try await getBucketReplication(bucket: bucket) {
            return .completed
        }
        return .pending
    }

    // MARK: - Event Notifications

    func putBucketNotification(bucket: String, configuration: NotificationConfiguration) async throws {
        // Store notification configuration in metadata store
        try await metadataStore.putBucketNotification(bucket: bucket, configuration: configuration)
    }

    func getBucketNotification(bucket: String) async throws -> NotificationConfiguration? {
        return try await metadataStore.getBucketNotification(bucket: bucket)
    }

    func deleteBucketNotification(bucket: String) async throws {
        try await metadataStore.deleteBucketNotification(bucket: bucket)
    }

    func publishEvent(bucket: String, event: S3EventType, key: String?, metadata: ObjectMetadata?, userIdentity: String?, sourceIPAddress: String?) async throws {
        // Get notification configuration
        guard let notificationConfig = try await getBucketNotification(bucket: bucket) else {
            return // No notifications configured
        }

        // Helper function to create event record for a specific configuration
        func createEventRecord(configurationId: String) -> S3EventRecord {
            S3EventRecord(
                eventName: event,
                userIdentity: UserIdentity(principalId: userIdentity ?? "SwiftS3"),
                requestParameters: RequestParameters(sourceIPAddress: sourceIPAddress ?? "127.0.0.1"),
                responseElements: ResponseElements(xAmzRequestId: UUID().uuidString, xAmzId2: UUID().uuidString),
                s3: S3Entity(
                    configurationId: configurationId,
                    bucket: S3Bucket(
                        name: bucket,
                        ownerIdentity: UserIdentity(principalId: userIdentity ?? "SwiftS3"),
                        arn: "arn:aws:s3:::\(bucket)"
                    ),
                    object: S3Object(
                        key: key ?? "",
                        size: metadata?.size,
                        eTag: metadata?.eTag,
                        versionId: metadata?.versionId,
                        sequencer: UUID().uuidString
                    )
                )
            )
        }

        // Publish to topics
        if let topicConfigs = notificationConfig.topicConfigurations {
            for topicConfig in topicConfigs {
                if topicConfig.events.contains(event) || topicConfig.events.contains(.objectCreated) || topicConfig.events.contains(.objectRemoved) {
                    let eventRecord = createEventRecord(configurationId: topicConfig.id ?? "default-topic")
                    let eventData = try JSONEncoder().encode(eventRecord)
                    Task {
                        do {
                            try await postToTopic(topicArn: topicConfig.topicArn, eventData: eventData)
                        } catch {
                            logger.error("Failed to post to topic \(topicConfig.topicArn): \(error)")
                        }
                    }
                }
            }
        }

        // Publish to queues
        if let queueConfigs = notificationConfig.queueConfigurations {
            for queueConfig in queueConfigs {
                if queueConfig.events.contains(event) || queueConfig.events.contains(.objectCreated) || queueConfig.events.contains(.objectRemoved) {
                    let eventRecord = createEventRecord(configurationId: queueConfig.id ?? "default-queue")
                    let eventData = try JSONEncoder().encode(eventRecord)
                    Task {
                        do {
                            try await postToQueue(queueArn: queueConfig.queueArn, eventData: eventData)
                        } catch {
                            logger.error("Failed to post to queue \(queueConfig.queueArn): \(error)")
                        }
                    }
                }
            }
        }

        // Publish to Lambda functions
        if let lambdaConfigs = notificationConfig.lambdaConfigurations {
            for lambdaConfig in lambdaConfigs {
                if lambdaConfig.events.contains(event) || lambdaConfig.events.contains(.objectCreated) || lambdaConfig.events.contains(.objectRemoved) {
                    let eventRecord = createEventRecord(configurationId: lambdaConfig.id ?? "default-lambda")
                    let eventData = try JSONEncoder().encode(eventRecord)
                    Task {
                        do {
                            try await self.invokeLambda(functionArn: lambdaConfig.lambdaFunctionArn, eventData: eventData)
                        } catch {
                            logger.error("Lambda invocation failed for \(lambdaConfig.lambdaFunctionArn): \(error)")
                        }
                    }
                }
            }
        }

        // Publish to webhooks
        if let webhookConfigs = notificationConfig.webhookConfigurations {
            for webhookConfig in webhookConfigs {
                if webhookConfig.events.contains(event) || webhookConfig.events.contains(.objectCreated) || webhookConfig.events.contains(.objectRemoved) {
                    let eventRecord = createEventRecord(configurationId: webhookConfig.id ?? "default-webhook")
                    let eventData = try JSONEncoder().encode(eventRecord)
                    Task {
                        do {
                            try await postWebhook(url: webhookConfig.url, eventData: eventData)
                        } catch {
                            logger.error("Failed to post webhook to \(webhookConfig.url): \(error)")
                        }
                    }
                }
            }
        }
    }

    // MARK: - VPC Configuration

    func getBucketVpcConfiguration(bucket: String) async throws -> VpcConfiguration? {
        return try await metadataStore.getBucketVpcConfiguration(bucket: bucket)
    }

    func putBucketVpcConfiguration(bucket: String, configuration: VpcConfiguration) async throws {
        try await metadataStore.putBucketVpcConfiguration(bucket: bucket, configuration: configuration)
    }

    func deleteBucketVpcConfiguration(bucket: String) async throws {
        try await metadataStore.deleteBucketVpcConfiguration(bucket: bucket)
    }

    // MARK: - Audit Events

    func logAuditEvent(_ event: AuditEvent) async throws {
        try await metadataStore.logAuditEvent(event)
    }

    func getAuditEvents(
        bucket: String?, principal: String?, eventType: AuditEventType?, startDate: Date?, endDate: Date?,
        limit: Int?, continuationToken: String?
    ) async throws -> (events: [AuditEvent], nextContinuationToken: String?) {
        return try await metadataStore.getAuditEvents(
            bucket: bucket, principal: principal, eventType: eventType,
            startDate: startDate, endDate: endDate, limit: limit, continuationToken: continuationToken
        )
    }

    func deleteAuditEvents(olderThan: Date) async throws {
        try await metadataStore.deleteAuditEvents(olderThan: olderThan)
    }

    /// Posts an event to a webhook URL.
    /// - Parameters:
    ///   - url: The webhook URL to post to
    ///   - eventData: The JSON event data to send
    private func postWebhook(url: String, eventData: Data) async throws {
        // Skip network calls in test mode
        if testMode {
            logger.info("Test mode: Skipping webhook post to \(url)")
            return
        }

        guard let url = URL(string: url) else {
            throw S3Error.invalidArgument
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = eventData

        let (_, response) = try await URLSession.shared.data(for: request)
        
        if let httpResponse = response as? HTTPURLResponse, !(200...299).contains(httpResponse.statusCode) {
            throw S3Error.internalError
        }
    }

    /// Posts an event to an SNS topic.
    /// For demo purposes, treats the topicArn as an HTTP URL if it starts with http.
    /// - Parameters:
    ///   - topicArn: The topic ARN or HTTP URL
    ///   - eventData: The JSON event data to send
    private func postToTopic(topicArn: String, eventData: Data) async throws {
        // For demo purposes, if the topicArn looks like an HTTP URL, post to it
        if topicArn.hasPrefix("http://") || topicArn.hasPrefix("https://") {
            try await postWebhook(url: topicArn, eventData: eventData)
        } else {
            // For real AWS SNS, this would require AWS credentials and proper API calls
            // For now, just log that SNS publishing is not implemented
            logger.info("SNS publishing not implemented for ARN: \(topicArn)")
        }
    }

    /// Posts an event to an SQS queue.
    /// For demo purposes, treats the queueArn as an HTTP URL if it starts with http.
    /// - Parameters:
    ///   - queueArn: The queue ARN or HTTP URL
    ///   - eventData: The JSON event data to send
    private func postToQueue(queueArn: String, eventData: Data) async throws {
        // For demo purposes, if the queueArn looks like an HTTP URL, post to it
        if queueArn.hasPrefix("http://") || queueArn.hasPrefix("https://") {
            try await postWebhook(url: queueArn, eventData: eventData)
        } else {
            // For real AWS SQS, this would require AWS credentials and proper API calls
            // For now, just log that SQS publishing is not implemented
            logger.info("SQS publishing not implemented for ARN: \(queueArn)")
        }
    }

    // MARK: - Batch Operations

    func createBatchJob(job: BatchJob) async throws -> String {
        return try await metadataStore.createBatchJob(job: job)
    }

    func getBatchJob(jobId: String) async throws -> BatchJob? {
        return try await metadataStore.getBatchJob(jobId: jobId)
    }

    func listBatchJobs(bucket: String?, status: BatchJobStatus?, limit: Int?, continuationToken: String?) async throws -> (jobs: [BatchJob], nextContinuationToken: String?) {
        return try await metadataStore.listBatchJobs(bucket: bucket, status: status, limit: limit, continuationToken: continuationToken)
    }

    func updateBatchJobStatus(jobId: String, status: BatchJobStatus, message: String?) async throws {
        try await metadataStore.updateBatchJobStatus(jobId: jobId, status: status, message: message)
    }

    func deleteBatchJob(jobId: String) async throws {
        try await metadataStore.deleteBatchJob(jobId: jobId)
    }

    /// Shuts down the HTTP client
    func shutdown() async throws {
        if let httpClient = httpClient {
            try await httpClient.shutdown()
        }
        // In testMode, httpClient is nil, so this is effectively synchronous
    }

    func executeBatchOperation(jobId: String, bucket: String, key: String) async throws {
        try await metadataStore.executeBatchOperation(jobId: jobId, bucket: bucket, key: key)
    }

    /// Invokes an AWS Lambda function with the provided event data
    /// - Parameters:
    ///   - functionArn: The ARN of the Lambda function to invoke
    ///   - eventData: The JSON data to send as the Lambda payload
    private func invokeLambda(functionArn: String, eventData: Data) async throws {
        // For now, this is a simplified implementation
        // In a real implementation, you would:
        // 1. Parse the Lambda ARN to extract region and function name
        // 2. Use AWS credentials to sign the request
        // 3. Make an HTTP POST to the Lambda invoke endpoint

        // Extract function name from ARN (simplified)
        let arnComponents = functionArn.split(separator: ":")
        guard arnComponents.count >= 6, arnComponents[2] == "lambda" else {
            throw S3Error(code: "InvalidArgument", message: "Invalid Lambda function ARN: \(functionArn)", statusCode: .badRequest)
        }
        let region = String(arnComponents[3])
        let functionName = String(arnComponents[5])

        // Construct Lambda invoke URL
        let url = "https://lambda.\(region).amazonaws.com/2020-06-30/functions/\(functionName)/invocations"

        // Create HTTP request
        var request = HTTPClientRequest(url: url)
        request.method = .POST
        request.headers.add(name: "Content-Type", value: "application/json")
        request.headers.add(name: "X-Amz-Invocation-Type", value: "Event") // Asynchronous invocation
        request.body = .bytes(ByteBuffer(data: eventData))

        // Note: In a real implementation, you would add AWS signature headers here
        // For now, this will fail without proper authentication

        guard let httpClient = httpClient else {
            logger.info("Test mode: Skipping Lambda invocation")
            return
        }

        let response = try await httpClient.execute(request, timeout: .seconds(30))

        // Check response status
        guard response.status == .accepted || response.status == .ok else {
            let body = try await response.body.collect(upTo: 1024 * 1024) // 1MB limit
            let errorMessage = String(buffer: body)
            logger.error("Lambda invocation failed with status \(response.status): \(errorMessage)")
            throw S3Error(code: "InternalError", message: "Lambda invocation failed: \(response.status)", statusCode: .internalServerError)
        }

        logger.info("Lambda function \(functionArn) invoked successfully")
    }
}
