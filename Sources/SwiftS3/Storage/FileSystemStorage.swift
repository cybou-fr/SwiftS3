import Crypto
import Foundation
import Hummingbird
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

actor FileSystemStorage: StorageBackend {
    let rootPath: FilePath
    let fileSystem = FileSystem.shared
    let metadataStore: MetadataStore

    init(rootPath: String, metadataStore: MetadataStore? = nil) {
        self.rootPath = FilePath(rootPath)
        self.metadataStore = metadataStore ?? FileSystemMetadataStore(rootPath: rootPath)
    }

    private func bucketPath(_ name: String) -> FilePath {
        return rootPath.appending(name)
    }

    private func getObjectPath(bucket: String, key: String) -> FilePath {
        return rootPath.appending(bucket).appending(key)
    }

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

    func createBucket(name: String) async throws {
        let path = bucketPath(name)
        if try await fileSystem.exists(at: path) {
            throw S3Error.bucketAlreadyExists
        }
        try await fileSystem.createDirectory(
            at: path, withIntermediateDirectories: true, permissions: nil)
    }

    func deleteBucket(name: String) async throws {
        let path = bucketPath(name)
        if !(try await fileSystem.exists(at: path)) {
            throw S3Error.noSuchBucket
        }

        // Check if empty
        let handle = try await fileSystem.openDirectory(atPath: path)
        let isEmpty: Bool
        do {
            var iterator = handle.listContents().makeAsyncIterator()
            isEmpty = try await iterator.next() == nil
            try await handle.close()
        } catch {
            try? await handle.close()
            throw error
        }

        if !isEmpty {
            throw S3Error.bucketNotEmpty
        }
        // recursive: false because we checked it's empty, but API requires arg
        _ = try? await fileSystem.removeItem(
            at: path, strategy: .platformDefault, recursively: false)
    }

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

    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: Stream, size: Int64?,
        metadata: [String: String]?
    ) async throws -> String where Stream.Element == ByteBuffer {
        let bPath = bucketPath(bucket)
        if !(try await fileSystem.exists(at: bPath)) {
            throw S3Error.noSuchBucket
        }

        let path = getObjectPath(bucket: bucket, key: key)

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
            customMetadata: metadata ?? [:]
        )
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: meta)

        return eTag
    }

    func getObject(bucket: String, key: String, range: ValidatedRange?) async throws -> (
        metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?
    ) {
        let path = getObjectPath(bucket: bucket, key: key)

        do {
            _ = try await fileSystem.info(forFileAt: path, infoAboutSymbolicLink: false)
        } catch {
            throw S3Error.noSuchKey
        }

        let metadata = try await getObjectMetadata(bucket: bucket, key: key)

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

    func deleteObject(bucket: String, key: String) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        _ = try? await fileSystem.removeItem(
            at: path, strategy: .platformDefault, recursively: false)
        try await metadataStore.deleteMetadata(bucket: bucket, key: key)
    }

    func deleteObjects(bucket: String, keys: [String]) async throws -> [String] {
        var deleted: [String] = []
        for key in keys {
            try? await deleteObject(bucket: bucket, key: key)
            deleted.append(key)
        }
        return deleted
    }

    func copyObject(fromBucket: String, fromKey: String, toBucket: String, toKey: String)
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
        if let srcMeta = try? await metadataStore.getMetadata(bucket: fromBucket, key: fromKey) {
            try await metadataStore.saveMetadata(bucket: toBucket, key: toKey, metadata: srcMeta)
        }

        return try await getObjectMetadata(bucket: toBucket, key: toKey)
    }

    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult {
        return try await metadataStore.listObjects(
            bucket: bucket, prefix: prefix, delimiter: delimiter, marker: marker,
            continuationToken: continuationToken, maxKeys: maxKeys)
    }

    func getObjectMetadata(bucket: String, key: String) async throws -> ObjectMetadata {
        return try await metadataStore.getMetadata(bucket: bucket, key: key)
    }

    // MARK: - Multipart Upload

    private func uploadPath(bucket: String, uploadId: String) -> FilePath {
        return bucketPath(bucket).appending(".uploads").appending(uploadId)
    }

    struct UploadInfo: Codable {
        let key: String
        let metadata: [String: String]?
    }

    func createMultipartUpload(bucket: String, key: String, metadata: [String: String]?)
        async throws
        -> String
    {
        let uploadId = UUID().uuidString
        let path = uploadPath(bucket: bucket, uploadId: uploadId)

        try await fileSystem.createDirectory(
            at: path, withIntermediateDirectories: true, permissions: nil)

        let info = UploadInfo(key: key, metadata: metadata)
        let data = try JSONEncoder().encode(info)

        // Write info.json
        _ = try await fileSystem.withFileHandle(
            forWritingAt: path.appending("info.json"), options: .newFile(replaceExisting: true)
        ) { handle in
            try await handle.write(contentsOf: data.map { $0 }, toAbsoluteOffset: 0)
        }

        return uploadId
    }

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
            customMetadata: info.metadata ?? [:]
        )
        try await metadataStore.saveMetadata(bucket: bucket, key: key, metadata: meta)

        // Cleanup
        _ = try? await fileSystem.removeItem(
            at: uPath, strategy: .platformDefault, recursively: true)

        return finalETag
    }

    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        _ = try? await fileSystem.removeItem(
            at: uPath, strategy: .platformDefault, recursively: true)
    }

    // MARK: - Bucket Policy

    func getBucketPolicy(bucket: String) async throws -> BucketPolicy {
        let policyPath = bucketPath(bucket).appending("policy.json")
        guard try await fileSystem.exists(at: policyPath) else {
            throw S3Error.noSuchBucketPolicy
        }

        let data = try await fileSystem.readAll(at: policyPath)
        return try JSONDecoder().decode(BucketPolicy.self, from: data)
    }

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
}
