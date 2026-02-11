import Crypto
import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO
import Testing

@testable import SwiftS3

@Suite("Policy Integration Tests")
struct PolicyIntegrationTests {

    static let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    static let threadPool: NIOThreadPool = {
        let tp = NIOThreadPool(numberOfThreads: 2)
        tp.start()
        return tp
    }()

    func withApp(
        users: [(accessKey: String, secretKey: String)] = [],
        _ test: @escaping @Sendable (any TestClientProtocol, SQLMetadataStore) async throws -> Void
    ) async throws {
        let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
            UUID().uuidString
        ).path
        let server = S3Server(
            hostname: "127.0.0.1", port: 0, storagePath: storagePath, accessKey: "admin",
            secretKey: "password")

        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite",
            on: Self.elg,
            threadPool: Self.threadPool
        )

        // Seed users
        for user in users {
            try await metadataStore.createUser(
                username: "User-\(user.accessKey)", accessKey: user.accessKey,
                secretKey: user.secretKey)
        }

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore)
        let controller = S3Controller(storage: storage)

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(userStore: metadataStore))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(server.hostname, port: server.port)),
            eventLoopGroupProvider: .shared(Self.elg)
        )

        try await app.test(.router) { client in
            try await test(client, metadataStore)
        }

        try? await metadataStore.shutdown()
        try? FileManager.default.removeItem(atPath: storagePath)
    }

    // Helper to generate auth header using AWSAuthHelper
    func sign(_ method: String, _ path: String, key: String, secret: String, body: String = "")
        -> HTTPFields
    {
        let helper = AWSAuthHelper(accessKey: key, secretKey: secret)
        // Construct standard URL for helper
        let url = URL(string: "http://localhost" + path)!

        // Map string method to HTTPRequest.Method
        let httpMethod = HTTPRequest.Method(rawValue: method) ?? .get

        do {
            return try helper.signRequest(method: httpMethod, url: url, payload: body)
        } catch {
            print("Signing failed: \(error)")
            return HTTPFields()
        }
    }

    @Test("Policy: Explicit Allow")
    func testExplicitAllow() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, store in
            // 1. Create Bucket
            _ = try await client.execute(
                uri: "/allow-bucket", method: .put,
                headers: sign("PUT", "/allow-bucket", key: "alice", secret: "secret1")
            )

            // 2. Put Policy: Allow "alice" to PutObject
            let policy = """
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "alice"},
                            "Action": "s3:PutObject",
                            "Resource": "arn:aws:s3:::allow-bucket/*"
                        }
                    ]
                }
                """

            let resPolicy = try await client.execute(
                uri: "/allow-bucket?policy", method: .put,
                headers: sign(
                    "PUT", "/allow-bucket?policy", key: "alice", secret: "secret1", body: policy),
                body: ByteBuffer(string: policy)
            )
            #expect(resPolicy.status == .noContent)

            // 3. PutObject as Alice -> Should Succeed
            let resPut = try await client.execute(
                uri: "/allow-bucket/test.txt", method: .put,
                headers: sign(
                    "PUT", "/allow-bucket/test.txt", key: "alice", secret: "secret1", body: "hello"),
                body: ByteBuffer(string: "hello")
            )
            #expect(resPut.status == .ok)
        }
    }

    @Test("Policy: Explicit Deny")
    func testExplicitDeny() async throws {
        try await withApp(users: [("bob", "secret2")]) { client, store in
            _ = try await client.execute(
                uri: "/deny-bucket", method: .put,
                headers: sign("PUT", "/deny-bucket", key: "bob", secret: "secret2")
            )

            // Deny Bob PutObject
            let policy = """
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": {"AWS": "bob"},
                            "Action": "s3:PutObject",
                            "Resource": "arn:aws:s3:::deny-bucket/*"
                        }
                    ]
                }
                """

            let resPolicy = try await client.execute(
                uri: "/deny-bucket?policy", method: .put,
                headers: sign(
                    "PUT", "/deny-bucket?policy", key: "bob", secret: "secret2", body: policy),
                body: ByteBuffer(string: policy)
            )
            #expect(resPolicy.status == .noContent)

            // PutObject as Bob -> Should Fail
            let resPut = try await client.execute(
                uri: "/deny-bucket/test.txt", method: .put,
                headers: sign(
                    "PUT", "/deny-bucket/test.txt", key: "bob", secret: "secret2", body: "hello"),
                body: ByteBuffer(string: "hello")
            )
            #expect(resPut.status == .forbidden)  // AccessDenied
        }
    }

    @Test("Policy: Implicit Deny (No Match)")
    func testImplicitDeny() async throws {
        try await withApp(users: [("alice", "secret1"), ("bob", "secret2")]) { client, store in
            // Alice creates bucket
            _ = try await client.execute(
                uri: "/implicit-bucket", method: .put,
                headers: sign("PUT", "/implicit-bucket", key: "alice", secret: "secret1")
            )

            // Policy allows Alice, but doesn't mention Bob.
            let policy = """
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "alice"},
                            "Action": "s3:*",
                            "Resource": "arn:aws:s3:::implicit-bucket/*"
                        }
                    ]
                }
                """

            _ = try await client.execute(
                uri: "/implicit-bucket?policy", method: .put,
                headers: sign(
                    "PUT", "/implicit-bucket?policy", key: "alice", secret: "secret1", body: policy),
                body: ByteBuffer(string: policy)
            )

            // Bob tries PutObject -> Should Fail (Implicit Deny) because policy exists and he's not in it.
            let resPut = try await client.execute(
                uri: "/implicit-bucket/test.txt", method: .put,
                headers: sign(
                    "PUT", "/implicit-bucket/test.txt", key: "bob", secret: "secret2", body: "hello"
                ),
                body: ByteBuffer(string: "hello")
            )
            #expect(resPut.status == .forbidden)
        }
    }
}
