import Crypto
import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO
import Testing

@testable import SwiftS3

@Suite("Presigned URL Integration Tests")
struct PresignedURLTests {

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
            configuration: .init(address: .hostname("127.0.0.1", port: 0)),
            eventLoopGroupProvider: .shared(Self.elg)
        )

        try await app.test(.router) { client in
            try await test(client, metadataStore)
        }

        try? await metadataStore.shutdown()
        try? FileManager.default.removeItem(atPath: storagePath)
    }

    @Test("Presigned URL: Valid Signature")
    func testValidPresignedURL() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, store in
            // Create bucket first using Alice (header auth)
            let helper = AWSAuthHelper(accessKey: "alice", secretKey: "secret1")
            _ = try await client.execute(
                uri: "/presign-bucket", method: .put,
                headers: helper.signRequest(
                    method: .put, url: URL(string: "http://localhost/presign-bucket")!)
            )
            // Alice puts object first (header auth)
            _ = try await client.execute(
                uri: "/presign-bucket/test.txt", method: .put,
                headers: helper.signRequest(
                    method: .put, url: URL(string: "http://localhost/presign-bucket/test.txt")!,
                    payload: "hello"),
                body: ByteBuffer(string: "hello")
            )

            // Generate "presigned" parameters manually to verify S3Authenticator's query logic
            let d = Date()
            let dateFormatter = DateFormatter()
            dateFormatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
            dateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
            let xAmzDate = dateFormatter.string(from: d)

            dateFormatter.dateFormat = "yyyyMMdd"
            let dateStamp = dateFormatter.string(from: d)

            let credential = "alice/\(dateStamp)/us-east-1/s3/aws4_request"

            // We use fixed components for the test
            let params = [
                "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
                "X-Amz-Credential": credential,
                "X-Amz-Date": xAmzDate,
                "X-Amz-Expires": "3600",
                "X-Amz-SignedHeaders": "host",
            ]
            let sortedQuery = params.sorted(by: { $0.key < $1.key })
                .map {
                    "\($0.key)=\($0.value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? $0.value)"
                }
                .joined(separator: "&")

            let uri = "/presign-bucket/test.txt"
            let canonicalRequest =
                "GET\n\(uri)\n\(sortedQuery)\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD"
            let credentialScope = "\(dateStamp)/us-east-1/s3/aws4_request"
            let stringToSign =
                "AWS4-HMAC-SHA256\n\(xAmzDate)\n\(credentialScope)\n\(SHA256.hash(data: Data(canonicalRequest.utf8)).hexString)"

            let kDate = try HMAC256.compute(dateStamp, key: "AWS4secret1")
            let kRegion = try HMAC256.compute("us-east-1", key: kDate)
            let kService = try HMAC256.compute("s3", key: kRegion)
            let kSigning = try HMAC256.compute("aws4_request", key: kService)
            let signature = try HMAC256.compute(stringToSign, key: kSigning).hexString

            let fullUri = "\(uri)?\(sortedQuery)&X-Amz-Signature=\(signature)"

            // Execute GET with Presigned URL (host header required)
            let res = try await client.execute(
                uri: fullUri, method: .get,
                headers: [.init("host")!: "localhost"]
            )

            #expect(res.status == .ok)
            #expect(String(buffer: res.body) == "hello")
        }
    }

    @Test("Presigned URL: Mutual Exclusivity")
    func testMutualExclusivity() async throws {
        try await withApp(users: [("alice", "secret1")]) { client, _ in
            let res = try await client.execute(
                uri: "/test?X-Amz-Algorithm=AWS4-HMAC-SHA256",
                method: .get,
                headers: [
                    .authorization: "AWS4-HMAC-SHA256 Credential=...",
                    .init("host")!: "localhost",
                ]
            )
            #expect(res.status == .badRequest)
        }
    }
}
