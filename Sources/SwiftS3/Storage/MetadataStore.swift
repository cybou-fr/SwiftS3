import Foundation
import NIO

/// Protocol defining metadata operations for S3 objects
protocol MetadataStore: Sendable {
    /// Retrieve metadata for an object
    func getMetadata(bucket: String, key: String) async throws -> ObjectMetadata
    
    /// Save metadata for an object
    func saveMetadata(bucket: String, key: String, metadata: ObjectMetadata) async throws
    
    /// Delete metadata for an object
    func deleteMetadata(bucket: String, key: String) async throws

    /// List objects in a bucket
    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult
}

/// Default implementation storing metadata in sidecar JSON files
struct FileSystemMetadataStore: MetadataStore {
    let rootPath: String
    private let fileManager = FileManager.default
    
    init(rootPath: String) {
        self.rootPath = rootPath
    }
    
    private func getObjectPath(bucket: String, key: String) -> String {
        return "\(rootPath)/\(bucket)/\(key)"
    }
    
    func getMetadata(bucket: String, key: String) async throws -> ObjectMetadata {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"
        
        guard fileManager.fileExists(atPath: path) else {
            throw S3Error.noSuchKey
        }
        
        // Read basic file attributes
        let attr = try fileManager.attributesOfItem(atPath: path)
        let size = attr[.size] as? Int64 ?? 0
        let date = attr[.modificationDate] as? Date ?? Date()
        
        // Read Custom Metadata from sidecar file
        var customMetadata: [String: String] = [:]
        var contentType: String? = nil
        
        if fileManager.fileExists(atPath: metaPath),
           let data = try? Data(contentsOf: URL(fileURLWithPath: metaPath)),
           let dict = try? JSONDecoder().decode([String: String].self, from: data) {
            customMetadata = dict
            contentType = dict["Content-Type"]
        }
        
        return ObjectMetadata(
            key: key,
            size: size,
            lastModified: date,
            eTag: nil, // ETag generation/storage can be improved later
            contentType: contentType,
            customMetadata: customMetadata
        )
    }
    
    func saveMetadata(bucket: String, key: String, metadata: ObjectMetadata) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"
        
        // Merge Content-Type into custom metadata for storage (simulating current behavior)
        var dict = metadata.customMetadata
        if let contentType = metadata.contentType {
            dict["Content-Type"] = contentType
        }
        
        let data = try JSONEncoder().encode(dict)
        try data.write(to: URL(fileURLWithPath: metaPath))
    }
    
    func deleteMetadata(bucket: String, key: String) async throws {
        let path = getObjectPath(bucket: bucket, key: key)
        let metaPath = path + ".metadata"
        
        if fileManager.fileExists(atPath: metaPath) {
            try fileManager.removeItem(atPath: metaPath)
        }
    }
    
    private func bucketPath(_ name: String) -> String {
        return "\(rootPath)/\(name)"
    }

    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
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

            // Ignore internal metadata files
            if relativeKey.hasSuffix(".metadata") || relativeKey.contains("/.uploads/") {
                continue
            }

            // Filter by Marker (V1) or ContinuationToken (V2)
            let startAfter = continuationToken ?? marker
            if let startAfter = startAfter, relativeKey <= startAfter {
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
        var nextContinuationToken: String? = nil

        let limit = maxKeys ?? 1000
        var count = 0

        // We need to keep track of the last seen "rolled up" prefix to avoid duplicates in the run
        var lastPrefix: String? = nil

        for obj in allObjects {
            if count >= limit {
                truncated = true
                nextMarker = objects.last?.key
                nextContinuationToken = objects.last?.key  // Use key as token for now
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
            nextMarker: nextMarker,
            nextContinuationToken: nextContinuationToken
        )
    }
}

