import Crypto
import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO
import Testing

@testable import SwiftS3

@Suite("Lifecycle Integration Tests")
struct LifecycleIntegrationTests {

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
            secretKey: "password", ldapConfig: nil)

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

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore, testMode: true)
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

    func sign(
        _ method: String, _ path: String, key: String = "admin", secret: String = "password",
        body: String = ""
    ) -> HTTPFields {
        let helper = AWSAuthHelper(accessKey: key, secretKey: secret)
        let url = URL(string: "http://localhost" + path)!
        let httpMethod = HTTPRequest.Method(rawValue: method) ?? .get
        return (try? helper.signRequest(method: httpMethod, url: url, payload: body))
            ?? HTTPFields()
    }

    @Test("Put and Get Lifecycle Configuration")
    func testPutGetLifecycle() async throws {
        try await withApp { client, store in
            let bucket = "lifecycle-bucket"

            // 1. Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { res in
                #expect(res.status == .ok)
            }

            // 2. Put Lifecycle
            let lifecycleXML = """
                <LifecycleConfiguration>
                    <Rule>
                        <ID>id1</ID>
                        <Filter>
                            <Prefix>logs/</Prefix>
                        </Filter>
                        <Status>Enabled</Status>
                        <Expiration>
                            <Days>30</Days>
                        </Expiration>
                    </Rule>
                </LifecycleConfiguration>
                """

            try await client.execute(
                uri: "/\(bucket)?lifecycle",
                method: .put,
                headers: sign("PUT", "/\(bucket)?lifecycle", body: lifecycleXML),
                body: ByteBuffer(string: lifecycleXML)
            ) { res in
                #expect(res.status == .ok)
            }

            // 3. Get Lifecycle
            try await client.execute(
                uri: "/\(bucket)?lifecycle",
                method: .get,
                headers: sign("GET", "/\(bucket)?lifecycle")
            ) { res in
                #expect(res.status == .ok)
                let body = String(buffer: res.body)
                #expect(body.contains("<ID>id1</ID>"))
                #expect(body.contains("<Prefix>logs/</Prefix>"))
                #expect(body.contains("<Days>30</Days>"))
            }
        }
    }

    @Test("Delete Lifecycle Configuration")
    func testDeleteLifecycle() async throws {
        try await withApp { client, store in
            let bucket = "delete-lifecycle-bucket"

            // 1. Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { res in
                #expect(res.status == .ok)
            }

            // 2. Put Lifecycle
            let lifecycleXML = """
                <LifecycleConfiguration>
                    <Rule>
                        <ID>tmp</ID>
                        <Filter><Prefix>tmp/</Prefix></Filter>
                        <Status>Enabled</Status>
                        <Expiration><Days>1</Days></Expiration>
                    </Rule>
                </LifecycleConfiguration>
                """
            try await client.execute(
                uri: "/\(bucket)?lifecycle",
                method: .put,
                headers: sign("PUT", "/\(bucket)?lifecycle", body: lifecycleXML),
                body: ByteBuffer(string: lifecycleXML)
            ) { res in
                #expect(res.status == .ok)
            }

            // 3. Delete Lifecycle
            try await client.execute(
                uri: "/\(bucket)?lifecycle",
                method: .delete,
                headers: sign("DELETE", "/\(bucket)?lifecycle")
            ) { res in
                #expect(res.status == .noContent)
            }

            // 4. Get Lifecycle (should be 404)
            try await client.execute(
                uri: "/\(bucket)?lifecycle",
                method: .get,
                headers: sign("GET", "/\(bucket)?lifecycle")
            ) { res in
                #expect(res.status == .notFound)
            }
        }
    }
}
