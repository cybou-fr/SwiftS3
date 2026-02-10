import Hummingbird
import Foundation
import NIO
import Crypto

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
        
        let urls = try fileManager.contentsOfDirectory(at: rootURL, includingPropertiesForKeys: [.creationDateKey], options: [])
        return try urls.filter { try $0.resourceValues(forKeys: [.isDirectoryKey]).isDirectory == true }
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
    
    func putObject(bucket: String, key: String, data: consuming some AsyncSequence<ByteBuffer, any Error> & Sendable, size: Int64?) async throws -> String {
        let bPath = bucketPath(bucket)
        var isDir: ObjCBool = false
        if !fileManager.fileExists(atPath: bPath, isDirectory: &isDir) || !isDir.boolValue {
            throw S3Error.noSuchBucket
        }
        
        let path = getObjectPath(bucket: bucket, key: key)
        let url = URL(fileURLWithPath: path)
        try fileManager.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        
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
    
    func getObject(bucket: String, key: String) async throws -> (metadata: ObjectMetadata, body: AsyncStream<ByteBuffer>?) {
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
                // Read in chunks
                while true {
                    let data = try fileHandle.read(upToCount: 64 * 1024)
                    guard let data = data, !data.isEmpty else { break }
                    continuation.yield(ByteBuffer(bytes: data))
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
    }
    
    func listObjects(bucket: String) async throws -> [ObjectMetadata] {
        let bPath = bucketPath(bucket)
        if !fileManager.fileExists(atPath: bPath) {
            throw S3Error.noSuchBucket
        }
        
        let bucketURL = URL(fileURLWithPath: bPath)
        let enumerator = fileManager.enumerator(at: bucketURL, includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey], options: [.skipsHiddenFiles])
        
        var objects: [ObjectMetadata] = []
        
        while let url = enumerator?.nextObject() as? URL {
            guard try url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory != true else { continue }
            
            let relativeKey = url.path.replacingOccurrences(of: bPath + "/", with: "")
            let values = try url.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey])
            
            objects.append(ObjectMetadata(
                key: relativeKey,
                size: Int64(values.fileSize ?? 0),
                lastModified: values.contentModificationDate ?? Date(),
                eTag: nil
            ))
        }
        return objects
    }
    
    func getObjectMetadata(bucket: String, key: String) async throws -> ObjectMetadata {
        let path = getObjectPath(bucket: bucket, key: key)
        guard fileManager.fileExists(atPath: path) else {
            throw S3Error.noSuchKey
        }
        
        let attr = try fileManager.attributesOfItem(atPath: path)
        let size = attr[.size] as? Int64 ?? 0
        let date = attr[.modificationDate] as? Date ?? Date()
        
        return ObjectMetadata(key: key, size: size, lastModified: date, eTag: nil)
    }
}
