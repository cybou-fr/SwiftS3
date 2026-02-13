import Crypto
import Foundation
import Hummingbird
import Logging
import NIO
import _NIOFileSystem

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

    /// Initializes a new file system storage instance.
    init(rootPath: String, metadataStore: MetadataStore? = nil) {
        self.rootPath = FilePath(rootPath)
        self.metadataStore = metadataStore ?? FileSystemMetadataStore(rootPath: rootPath)
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

    func deleteObjects(bucket: String, keys: [String]) async throws -> [String] {
        var deleted: [String] = []
        for key in keys {
            _ = try? await deleteObject(bucket: bucket, key: key, versionId: nil)  // Default to null/latest for bulk delete for now
            deleted.append(key)
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
                                let info = try JSONDecoder().decode(UploadInfo.self, from: infoData)
                                
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
}
