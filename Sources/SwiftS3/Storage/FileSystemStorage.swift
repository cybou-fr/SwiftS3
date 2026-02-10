import Crypto
import Foundation
import Hummingbird
import NIO

actor FileSystemStorage: StorageBackend {
    let rootPath: String
    let fileManager = FileManager.default

    init(rootPath: String) {
        self.rootPath = rootPath
    }

    private func bucketPath(_ name: String) -> String {
        return "\(rootPath)/\(name)"
    }

    private func getObjectPath(bucket: String, key: String) -> String {
        return "\(rootPath)/\(bucket)/\(key)"
    }

    func listBuckets() async throws -> [(name: String, created: Date)] {
        let rootURL = URL(fileURLWithPath: rootPath)
        // Create root if not exists
        if !fileManager.fileExists(atPath: rootPath) {
            try fileManager.createDirectory(at: rootURL, withIntermediateDirectories: true)
        }

        let urls = try fileManager.contentsOfDirectory(
            at: rootURL, includingPropertiesForKeys: [.creationDateKey], options: [])
        return try urls.filter {
            try $0.resourceValues(forKeys: [.isDirectoryKey]).isDirectory == true
        }
        .map { url in
            let values = try url.resourceValues(forKeys: [.creationDateKey])
            return (name: url.lastPathComponent, created: values.creationDate ?? Date())
        }
    }

    func createBucket(name: String) async throws {
        let path = bucketPath(name)
        if fileManager.fileExists(atPath: path) {
            throw S3Error.bucketAlreadyExists
        }
        try fileManager.createDirectory(atPath: path, withIntermediateDirectories: true)
    }

    func deleteBucket(name: String) async throws {
        let path = bucketPath(name)
        if !fileManager.fileExists(atPath: path) {
            throw S3Error.noSuchBucket
        }
        // Check if empty
        let contents = try fileManager.contentsOfDirectory(atPath: path)
        if !contents.isEmpty {
            throw S3Error.bucketNotEmpty
        }
        try fileManager.removeItem(atPath: path)
    }

    func headBucket(name: String) async throws {
        let path = bucketPath(name)
        var isDir: ObjCBool = false
        if !fileManager.fileExists(atPath: path, isDirectory: &isDir) || !isDir.boolValue {
            throw S3Error.noSuchBucket
        }
    }

    func putObject<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, data: consuming Stream, size: Int64?,
        metadata: [String: String]?
    ) async throws -> String where Stream.Element == ByteBuffer {
        let bPath = bucketPath(bucket)
        var isDir: ObjCBool = false
        if !fileManager.fileExists(atPath: bPath, isDirectory: &isDir) || !isDir.boolValue {
            throw S3Error.noSuchBucket
        }

        let path = getObjectPath(bucket: bucket, key: key)
        let url = URL(fileURLWithPath: path)
        try fileManager.createDirectory(
            at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

        // Write Metadata
        if let metadata = metadata {
            let metaPath = path + ".metadata"
            let metaData = try JSONEncoder().encode(metadata)
            try metaData.write(to: URL(fileURLWithPath: metaPath))
        }

        // Create file
        if !fileManager.createFile(atPath: path, contents: nil) {
            // If it fails, maybe it already exists, checking write permissions?
            // Actually createFile returns true if successful. If it fails, we might throw.
        }

        let fileHandle = try FileHandle(forWritingTo: url)
        defer { try? fileHandle.close() }

        var digest = SHA256()

        for try await var buffer in data {
            let bytes = buffer.readBytes(length: buffer.readableBytes) ?? []
            let data = Data(bytes)
            digest.update(data: data)
            fileHandle.write(data)
        }

        return digest.finalize().map { String(format: "%02x", $0) }.joined()
    }

    func getObject(bucket: String, key: String, range: ValidatedRange?) async throws -> (
        metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?
    ) {
        let path = getObjectPath(bucket: bucket, key: key)
        guard fileManager.fileExists(atPath: path) else {
            throw S3Error.noSuchKey
        }

        let metadata = try await getObjectMetadata(bucket: bucket, key: key)

        // Stream the file using a detached task to avoid blocking the actor
        let body = AsyncStream<ByteBuffer> { continuation in
            let fileURL = URL(fileURLWithPath: path)
            do {
                let fileHandle = try FileHandle(forReadingFrom: fileURL)

                if let range = range {
                    try fileHandle.seek(toOffset: UInt64(range.start))
                    var remaining = range.end - range.start + 1

                    while remaining > 0 {
                        let chunkSize = min(remaining, 64 * 1024)
                        let data = try fileHandle.read(upToCount: Int(chunkSize))
                        guard let data = data, !data.isEmpty else { break }
                        continuation.yield(ByteBuffer(bytes: data))
                        remaining -= Int64(data.count)
                    }
                } else {
                    // Read in chunks
                    while true {
                        let data = try fileHandle.read(upToCount: 64 * 1024)
                        guard let data = data, !data.isEmpty else { break }
                        continuation.yield(ByteBuffer(bytes: data))
                    }
                }

                try? fileHandle.close()
                continuation.finish()
            } catch {
                continuation.finish()
            }
        }

        return (metadata, body)
    }

    func deleteObject(bucket: String, key: String) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        if fileManager.fileExists(atPath: path) {
            try fileManager.removeItem(atPath: path)
        }
        let metaPath = path + ".metadata"
        if fileManager.fileExists(atPath: metaPath) {
            try? fileManager.removeItem(atPath: metaPath)
        }
    }

    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult {
        let bPath = bucketPath(bucket)
        if !fileManager.fileExists(atPath: bPath) {
            throw S3Error.noSuchBucket
        }

        let bucketURL = URL(fileURLWithPath: bPath).standardizedFileURL
        let bucketPathString = bucketURL.path

        let enumerator = fileManager.enumerator(
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

            // Filter by Marker (lexicographical > marker)
            if let marker = marker, relativeKey <= marker {
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

        let limit = maxKeys ?? 1000
        var count = 0

        // We need to keep track of the last seen "rolled up" prefix to avoid duplicates in the run
        var lastPrefix: String? = nil

        for obj in allObjects {
            if count >= limit {
                truncated = true
                nextMarker = objects.last?.key
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
            nextMarker: nextMarker
        )
    }

    func getObjectMetadata(bucket: String, key: String) async throws -> ObjectMetadata {
        let path = getObjectPath(bucket: bucket, key: key)
        guard fileManager.fileExists(atPath: path) else {
            throw S3Error.noSuchKey
        }

        let attr = try fileManager.attributesOfItem(atPath: path)
        let size = attr[.size] as? Int64 ?? 0
        let date = attr[.modificationDate] as? Date ?? Date()

        // Read Metadata
        var customMetadata: [String: String] = [:]
        var contentType: String? = nil

        let metaPath = path + ".metadata"
        if fileManager.fileExists(atPath: metaPath),
            let data = try? Data(contentsOf: URL(fileURLWithPath: metaPath)),
            let dict = try? JSONDecoder().decode([String: String].self, from: data)
        {
            customMetadata = dict
            contentType = dict["Content-Type"]
        }

        return ObjectMetadata(
            key: key, size: size, lastModified: date, eTag: nil, contentType: contentType,
            customMetadata: customMetadata)
    }

    // MARK: - Multipart Upload

    private func uploadPath(bucket: String, uploadId: String) -> String {
        return bucketPath(bucket) + "/.uploads/" + uploadId
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

        try fileManager.createDirectory(atPath: path, withIntermediateDirectories: true)

        let info = UploadInfo(key: key, metadata: metadata)
        let data = try JSONEncoder().encode(info)
        try data.write(to: URL(fileURLWithPath: path + "/info.json"))

        return uploadId
    }

    func uploadPart<Stream: AsyncSequence & Sendable>(
        bucket: String, key: String, uploadId: String, partNumber: Int, data: consuming Stream,
        size: Int64?
    ) async throws -> String where Stream.Element == ByteBuffer {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        if !fileManager.fileExists(atPath: uPath) {
            throw S3Error.noSuchUpload
        }

        let partPath = uPath + "/\(partNumber)"
        let fileURL = URL(fileURLWithPath: partPath)

        if !fileManager.createFile(atPath: partPath, contents: nil) {
            // Handle error
        }

        let fileHandle = try FileHandle(forWritingTo: fileURL)
        defer { try? fileHandle.close() }

        var digest = SHA256()

        for try await var buffer in data {
            let bytes = buffer.readBytes(length: buffer.readableBytes) ?? []
            let data = Data(bytes)
            digest.update(data: data)
            fileHandle.write(data)
        }

        return digest.finalize().map { String(format: "%02x", $0) }.joined()
    }

    func completeMultipartUpload(
        bucket: String, key: String, uploadId: String, parts: [PartInfo]
    ) async throws -> String {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        if !fileManager.fileExists(atPath: uPath) {
            throw S3Error.noSuchUpload
        }

        // Read Info
        let infoData = try Data(contentsOf: URL(fileURLWithPath: uPath + "/info.json"))
        let info = try JSONDecoder().decode(UploadInfo.self, from: infoData)

        guard info.key == key else {
            throw S3Error.invalidPart  // Key mismatch
        }

        // Sort parts by number (S3 spec says client provides them sorted, but we should ensure or trust)
        // We will trust the input list order as per S3 `CompleteMultipartUpload` spec usually requires it,
        // but verifying existence is good.

        let finalPath = getObjectPath(bucket: bucket, key: key)
        let finalURL = URL(fileURLWithPath: finalPath)

        // Ensure parent dir exists
        try fileManager.createDirectory(
            at: finalURL.deletingLastPathComponent(), withIntermediateDirectories: true)

        if !fileManager.createFile(atPath: finalPath, contents: nil) {

        }
        let fileHandle = try FileHandle(forWritingTo: finalURL)
        defer { try? fileHandle.close() }

        var fullDigest = SHA256()  // S3 ETag for multipart is usually -N, but we can compute full hash or just mock it.
        // For simplicity, we'll return a combined hash or just a unique string.
        // S3 ETag for multipart: Hex(MD5(Part1) + ... + MD5(PartN)) + "-" + N
        // We'll mimic this with SHA256 for now or just return a simple UUID/Hash.

        for part in parts {
            let partPath = uPath + "/\(part.partNumber)"
            guard fileManager.fileExists(atPath: partPath) else {
                throw S3Error.invalidPart
            }

            let partData = try Data(contentsOf: URL(fileURLWithPath: partPath))
            fileHandle.write(partData)
            fullDigest.update(data: partData)
        }

        let finalETag =
            fullDigest.finalize().map { String(format: "%02x", $0) }.joined() + "-\(parts.count)"

        // Write Metadata
        if let metadata = info.metadata {
            let metaPath = finalPath + ".metadata"
            let metaData = try JSONEncoder().encode(metadata)
            try metaData.write(to: URL(fileURLWithPath: metaPath))
        }

        // Cleanup
        try fileManager.removeItem(atPath: uPath)

        return finalETag
    }

    func abortMultipartUpload(bucket: String, key: String, uploadId: String) async throws {
        let uPath = uploadPath(bucket: bucket, uploadId: uploadId)
        if fileManager.fileExists(atPath: uPath) {
            try fileManager.removeItem(atPath: uPath)
        }
    }
}
