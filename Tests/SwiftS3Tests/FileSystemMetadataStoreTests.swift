import Foundation
import NIO
import Testing

@testable import SwiftS3

@Suite("FileSystem Metadata Store Tests")
struct FileSystemMetadataStoreTests {

    @Test("FileSystemMetadataStore: Basic CRUD")
    func testFileSystemMetadataStoreCRUD() async throws {
        let tempDir = FileManager.default.temporaryDirectory.path + "/test-meta-\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let store = FileSystemMetadataStore(rootPath: tempDir)

        // Create bucket directory and dummy file
        try FileManager.default.createDirectory(atPath: "\(tempDir)/test-bucket", withIntermediateDirectories: true)
        let filePath = "\(tempDir)/test-bucket/test-file.txt"
        try "dummy content".write(to: URL(fileURLWithPath: filePath), atomically: true, encoding: .utf8)

        let metadata = ObjectMetadata(
            key: "test-file.txt",
            size: 100,
            lastModified: Date(),
            eTag: "test-etag",
            contentType: "text/plain",
            customMetadata: ["custom": "value"],
            owner: "test-owner",
            versionId: "null",
            isLatest: true,
            isDeleteMarker: false
        )

        // Save metadata
        try await store.saveMetadata(bucket: "test-bucket", key: "test-file.txt", metadata: metadata)

        // Retrieve metadata
        let retrieved = try await store.getMetadata(bucket: "test-bucket", key: "test-file.txt", versionId: nil)

        #expect(retrieved.key == "test-file.txt")
        #expect(retrieved.size == 13)  // "dummy content" is 13 bytes
        #expect(retrieved.contentType == "text/plain")
        #expect(retrieved.customMetadata["custom"] == "value")

        // Delete metadata and file
        try await store.deleteMetadata(bucket: "test-bucket", key: "test-file.txt", versionId: nil)
        try FileManager.default.removeItem(atPath: filePath)

        // Should throw after deletion
        await #expect(throws: S3Error.self) {
            _ = try await store.getMetadata(bucket: "test-bucket", key: "test-file.txt", versionId: nil)
        }
    }

    @Test("FileSystemMetadataStore: ACL Operations")
    func testFileSystemMetadataStoreACL() async throws {
        let tempDir = FileManager.default.temporaryDirectory.path + "/test-acl-\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: tempDir) }

        let store = FileSystemMetadataStore(rootPath: tempDir)

        // Create bucket directory
        try FileManager.default.createDirectory(atPath: "\(tempDir)/acl-bucket", withIntermediateDirectories: true)

        let acl = AccessControlPolicy(
            owner: Owner(id: "test-owner"),
            accessControlList: [
                Grant(grantee: Grantee.user(id: "test-user"), permission: .read)
            ]
        )

        // Put ACL
        try await store.putACL(bucket: "acl-bucket", key: nil, versionId: nil, acl: acl)

        // Get ACL
        let retrievedACL = try await store.getACL(bucket: "acl-bucket", key: nil, versionId: nil)

        #expect(retrievedACL.owner.id == "test-owner")
        #expect(retrievedACL.accessControlList.count == 1)
        #expect(retrievedACL.accessControlList[0].grantee.id == "test-user")
        #expect(retrievedACL.accessControlList[0].permission == .read)
    }

    // Note: Tagging and Lifecycle are not implemented in FileSystemMetadataStore
    // They return empty results or no-ops
}