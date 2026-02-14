import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO
import Testing

@testable import SwiftS3

@Suite("Versioning Tests")
struct VersioningTests {

    // Shared resources
    static let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    static let threadPool: NIOThreadPool = {
        let tp = NIOThreadPool(numberOfThreads: 2)
        tp.start()
        return tp
    }()

    // MARK: - Helper
    func withApp(_ test: @escaping @Sendable (any TestClientProtocol) async throws -> Void)
        async throws
    {
        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        let server = S3Server(
            hostname: "127.0.0.1", port: 0, storagePath: storagePath, accessKey: "admin",
            secretKey: "password", ldapConfig: nil)

        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite",
            on: Self.elg,
            threadPool: Self.threadPool
        )

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)
        let controller = S3Controller(storage: storage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(userStore: metadataStore))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(server.hostname, port: server.port))
        )

        try await app.test(.router, test)

        try? await storage.shutdown()
        try? await metadataStore.shutdown()
        try? FileManager.default.removeItem(atPath: storagePath)
    }

    @Test("Versioning Flow")
    func testVersioningFlow() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()
            let bucketUrl = URL(string: "http://localhost:8080/versioned-bucket")!

            // 1. Create Bucket
            let createHeaders = try helper.signRequest(method: .put, url: bucketUrl)
            try await app.execute(uri: "/versioned-bucket", method: .put, headers: createHeaders) {
                response in
                #expect(response.status == .ok)
            }

            // 2. Enable Versioning
            let versioningConfig = """
                <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <Status>Enabled</Status>
                </VersioningConfiguration>
                """
            let versioningUrl = URL(string: "http://localhost:8080/versioned-bucket?versioning")!
            let verHeaders = try helper.signRequest(
                method: .put, url: versioningUrl, payload: versioningConfig)
            try await app.execute(
                uri: "/versioned-bucket?versioning", method: .put, headers: verHeaders,
                body: ByteBuffer(string: versioningConfig)
            ) { response in
                #expect(response.status == .ok)
            }

            // 3. Put Object v1
            let objectUrl = URL(string: "http://localhost:8080/versioned-bucket/file.txt")!
            let bodyV1 = "Version 1"
            let putV1Headers = try helper.signRequest(method: .put, url: objectUrl, payload: bodyV1)
            var versionId1 = ""
            try await app.execute(
                uri: "/versioned-bucket/file.txt", method: .put, headers: putV1Headers,
                body: ByteBuffer(string: bodyV1)
            ) { response in
                #expect(response.status == .ok)
                versionId1 = response.headers[.init("x-amz-version-id")!] ?? ""
                #expect(!versionId1.isEmpty && versionId1 != "null")
            }

            // 4. Put Object v2
            let bodyV2 = "Version 2"
            let putV2Headers = try helper.signRequest(method: .put, url: objectUrl, payload: bodyV2)
            var versionId2 = ""
            try await app.execute(
                uri: "/versioned-bucket/file.txt", method: .put, headers: putV2Headers,
                body: ByteBuffer(string: bodyV2)
            ) { response in
                #expect(response.status == .ok)
                versionId2 = response.headers[.init("x-amz-version-id")!] ?? ""
                #expect(!versionId2.isEmpty)
                #expect(versionId1 != versionId2)
            }

            // 5. Get Object (Latest -> v2)
            let getHeaders = try helper.signRequest(method: .get, url: objectUrl)
            try await app.execute(
                uri: "/versioned-bucket/file.txt", method: .get, headers: getHeaders
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body == bodyV2)
                #expect(response.headers[.init("x-amz-version-id")!] == versionId2)
            }

            // 6. Get Object v1
            let v1Url = URL(
                string: "http://localhost:8080/versioned-bucket/file.txt?versionId=\(versionId1)")!
            let getV1Headers = try helper.signRequest(method: .get, url: v1Url)
            try await app.execute(
                uri: "/versioned-bucket/file.txt?versionId=\(versionId1)", method: .get,
                headers: getV1Headers
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body == bodyV1)
                #expect(response.headers[.init("x-amz-version-id")!] == versionId1)
            }

            // 7. List Versions
            let listUrl = URL(string: "http://localhost:8080/versioned-bucket?versions")!
            let listHeaders = try helper.signRequest(method: .get, url: listUrl)
            try await app.execute(
                uri: "/versioned-bucket?versions", method: .get, headers: listHeaders
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body.contains(versionId1))
                #expect(body.contains(versionId2))
                #expect(body.contains("<Version>"))
            }

            // 8. Delete Object (Latest)
            let deleteHeaders = try helper.signRequest(method: .delete, url: objectUrl)
            var deleteMarkerVersionId = ""
            try await app.execute(
                uri: "/versioned-bucket/file.txt", method: .delete, headers: deleteHeaders
            ) { response in
                // S3 Delete usually returns 204 No Content
                #expect(response.status == .noContent || response.status == .ok)
                deleteMarkerVersionId = response.headers[.init("x-amz-version-id")!] ?? ""
                #expect(!deleteMarkerVersionId.isEmpty)
            }

            // 9. Get Object (Latest -> 404)
            // Note: After delete marker, get should 404 unless we request specific version
            try await app.execute(
                uri: "/versioned-bucket/file.txt", method: .get, headers: getHeaders
            ) { response in
                #expect(response.status == .notFound)
            }

            // 10. List Versions (Check Delete Marker)
            try await app.execute(
                uri: "/versioned-bucket?versions", method: .get, headers: listHeaders
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)
                #expect(body.contains("<DeleteMarker>"))
                #expect(body.contains(deleteMarkerVersionId))
            }
        }
    }

    @Test("List Versions with Delimiter")
    func testVersioningDelimiter() async throws {
        try await withApp { app in
            let helper = AWSAuthHelper()
            let bucket = "delim-version-bucket"

            // 1. Create Bucket
            _ = try await app.execute(
                uri: "/\(bucket)", method: .put,
                headers: try helper.signRequest(
                    method: .put, url: URL(string: "http://localhost/\(bucket)")!)
            )

            // 2. Put Objects in "folders"
            let keys = ["logs/1.txt", "logs/2.txt", "data/test.txt", "root.txt"]
            for key in keys {
                let url = URL(string: "http://localhost/\(bucket)/\(key)")!
                _ = try await app.execute(
                    uri: "/\(bucket)/\(key)", method: .put,
                    headers: try helper.signRequest(method: .put, url: url, payload: "data"),
                    body: ByteBuffer(string: "data")
                )
            }

            // 3. List Versions with delimiter="/"
            let listUrl = URL(string: "http://localhost/\(bucket)?versions&delimiter=/")!
            let listHeaders = try helper.signRequest(method: .get, url: listUrl)
            try await app.execute(
                uri: "/\(bucket)?versions&delimiter=/", method: .get, headers: listHeaders
            ) { response in
                #expect(response.status == .ok)
                let body = String(buffer: response.body)

                // Should contain "root.txt" as Version
                #expect(body.contains("<Key>root.txt</Key>"))

                // Should contain "logs/" and "data/" as CommonPrefixes
                #expect(body.contains("<CommonPrefixes>"))
                #expect(body.contains("<Prefix>logs/</Prefix>"))
                #expect(body.contains("<Prefix>data/</Prefix>"))

                // Should NOT contain the files inside folders as top-level Versions
                #expect(!body.contains("<Key>logs/1.txt</Key>"))
            }
        }
    }
}
