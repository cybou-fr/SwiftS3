import Crypto
import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO
import Testing

@testable import SwiftS3

@Suite("ACL Integration Tests")
struct ACLIntegrationTests {

    func withApp(
        users: [(accessKey: String, secretKey: String)] = [],
        _ test: @escaping @Sendable (any TestClientProtocol, SQLMetadataStore) async throws -> Void
    ) async throws {
        // Create per-test event loop group and thread pool
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

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
            on: elg,
            threadPool: threadPool
        )

        // Seed users
        for user in users {
            try await metadataStore.createUser(
                username: "User-\(user.accessKey)", accessKey: user.accessKey,
                secretKey: user.secretKey)
        }

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)
        let controller = S3Controller(storage: storage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(userStore: metadataStore))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(server.hostname, port: server.port)),
            eventLoopGroupProvider: .shared(elg)
        )

        do {
            try await app.test(.router) { client in
                try await test(client, metadataStore)
            }
        } catch {
            // Cleanup on error
            try? await storage.shutdown()
            try? await metadataStore.shutdown()
            try? FileManager.default.removeItem(atPath: storagePath)
            try? await threadPool.shutdownGracefully()
            try? await elg.shutdownGracefully()
            throw error
        }

        // Cleanup
        try? await storage.shutdown()
        try? await metadataStore.shutdown()
        try? FileManager.default.removeItem(atPath: storagePath)
        try? await threadPool.shutdownGracefully()
        try? await elg.shutdownGracefully()
    }

    // Helper to generate auth header using AWSAuthHelper (copied/adapted from PolicyIntegrationTests)
    func sign(_ method: String, _ path: String, key: String, secret: String, body: String = "")
        -> HTTPFields
    {
        let helper = AWSAuthHelper(accessKey: key, secretKey: secret)
        let url = URL(string: "http://localhost" + path)!
        let httpMethod = HTTPRequest.Method(rawValue: method) ?? .get
        do {
            return try helper.signRequest(method: httpMethod, url: url, payload: body)
        } catch {
            print("Signing failed: \(error)")
            return HTTPFields()
        }
    }

    @Test("ACL: Default Owner Access")
    func testDefaultOwnerAccess() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, store in
            // Alice creates bucket
            let resCreate = try await client.execute(
                uri: "/alice-bucket", method: .put,
                headers: sign("PUT", "/alice-bucket", key: "alice", secret: "secret1")
            )
            #expect(resCreate.status == .ok)

            // Alice puts object
            let resPut = try await client.execute(
                uri: "/alice-bucket/test.txt", method: .put,
                headers: sign(
                    "PUT", "/alice-bucket/test.txt", key: "alice", secret: "secret1", body: "data"),
                body: ByteBuffer(string: "data")
            )
            #expect(resPut.status == .ok)

            // Alice gets object -> Should Succeed (Implicit Owner Access)
            let resGet = try await client.execute(
                uri: "/alice-bucket/test.txt", method: .get,
                headers: sign("GET", "/alice-bucket/test.txt", key: "alice", secret: "secret1")
            )
            #expect(resGet.status == .ok)
        }
    }

    @Test("ACL: Anonymous Access Denied by Default")
    func testAnonymousDenied() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, store in
            // Alice creates bucket
            _ = try await client.execute(
                uri: "/private-bucket", method: .put,
                headers: sign("PUT", "/private-bucket", key: "alice", secret: "secret1")
            )

            // Alice puts object
            _ = try await client.execute(
                uri: "/private-bucket/secret.txt", method: .put,
                headers: sign(
                    "PUT", "/private-bucket/secret.txt", key: "alice", secret: "secret1",
                    body: "secret"),
                body: ByteBuffer(string: "secret")
            )

            // Anonymous GET -> Should Fail (403 or 404, usually 403 if default deny)
            // My implementation returns 403 (AccessDenied) in checkAccess if implicit deny.
            let resGet = try await client.execute(
                uri: "/private-bucket/secret.txt", method: .get
            )
            #expect(resGet.status == .forbidden)
        }
    }

    @Test("ACL: Public Read Canned ACL")
    func testPublicRead() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, store in
            // 1. Create Bucket
            _ = try await client.execute(
                uri: "/public-bucket", method: .put,
                headers: sign("PUT", "/public-bucket", key: "alice", secret: "secret1")
            )

            // 2. Alice puts object with public-read ACL
            var headers = sign(
                "PUT", "/public-bucket/public.txt", key: "alice", secret: "secret1", body: "public")
            headers[HTTPField.Name("x-amz-acl")!] = "public-read"

            let resPut = try await client.execute(
                uri: "/public-bucket/public.txt", method: .put,
                headers: headers,
                body: ByteBuffer(string: "public")
            )
            #expect(resPut.status == .ok)

            // 3. Anonymous GET -> Should Succeed
            let resGet = try await client.execute(
                uri: "/public-bucket/public.txt", method: .get
            )
            #expect(resGet.status == .ok)
            let body = String(buffer: resGet.body)
            #expect(body == "public")
        }
    }

    @Test("ACL: Get/Put Object ACL")
    func testGetPutObjectACL() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, store in
            // Alice creates bucket and object (private default)
            _ = try await client.execute(
                uri: "/acl-bucket", method: .put,
                headers: sign("PUT", "/acl-bucket", key: "alice", secret: "secret1")
            )
            _ = try await client.execute(
                uri: "/acl-bucket/obj", method: .put,
                headers: sign(
                    "PUT", "/acl-bucket/obj", key: "alice", secret: "secret1", body: "data"),
                body: ByteBuffer(string: "data")
            )

            // 1. Get ACL -> Should contain Owner and Grants (Owner FULL_CONTROL)
            let resGetACL = try await client.execute(
                uri: "/acl-bucket/obj?acl", method: .get,
                headers: sign("GET", "/acl-bucket/obj?acl", key: "alice", secret: "secret1")
            )
            #expect(resGetACL.status == .ok)
            let xml = String(buffer: resGetACL.body)
            #expect(xml.contains("<ID>alice</ID>"))
            #expect(xml.contains("<Permission>FULL_CONTROL</Permission>"))

            // 2. Put ACL (Canned public-read) via ?acl + Header
            var putHeaders = sign("PUT", "/acl-bucket/obj?acl", key: "alice", secret: "secret1")
            putHeaders[HTTPField.Name("x-amz-acl")!] = "public-read"

            let resPutACL = try await client.execute(
                uri: "/acl-bucket/obj?acl", method: .put,
                headers: putHeaders
            )
            #expect(resPutACL.status == .ok)

            // 3. Validate Public Access
            let resAnonCheck = try await client.execute(uri: "/acl-bucket/obj", method: .get)
            #expect(resAnonCheck.status == .ok)
        }
    }
}
