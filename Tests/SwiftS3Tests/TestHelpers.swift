import Crypto
import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import SQLiteNIO

@testable import SwiftS3

/// Helper to generate AWS Signature V4 headers for testing
struct AWSAuthHelper {
    let accessKey: String
    let secretKey: String
    let region: String
    let service: String

    init(
        accessKey: String = "admin", secretKey: String = "password", region: String = "us-east-1",
        service: String = "s3"
    ) {
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.region = region
        self.service = service
    }

    func signRequest(
        method: HTTPRequest.Method,
        url: URL,  // URL must contain scheme, host, port (if any), path, and query
        payload: String = "",  // Empty string for empty body
        date: Date = Date()
    ) throws -> HTTPFields {
        let methodStr = method.rawValue
        let path = url.path
        let query = url.query ?? ""
        let host = url.host ?? "localhost"
        let port = url.port

        let isoDateFormatter = ISO8601DateFormatter()
        isoDateFormatter.formatOptions = [
            .withYear, .withMonth, .withDay, .withTime, .withTimeZone,
        ]
        // AWS requires basic format: YYYYMMDDTHHMMSSZ, ISO8601 usually has separators
        // Custom formatter for AWS
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        dateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        let amzDate = dateFormatter.string(from: date)

        dateFormatter.dateFormat = "yyyyMMdd"
        let dateStamp = dateFormatter.string(from: date)

        // 1. Canonical Headers
        var canonicalHeaders = ""
        var signedHeaders = ""

        // Host header
        var hostHeader = host
        if let port = port {
            hostHeader += ":\(port)"
        }

        // Payload Hash (compute first, needed for canonical headers)
        let payloadData = Data(payload.utf8)
        let payloadHash = SHA256.hash(data: payloadData).map { String(format: "%02x", $0) }.joined()

        canonicalHeaders += "host:\(hostHeader)\n"
        canonicalHeaders += "x-amz-content-sha256:\(payloadHash)\n"
        canonicalHeaders += "x-amz-date:\(amzDate)\n"
        signedHeaders = "host;x-amz-content-sha256;x-amz-date"

        // Canonical Request
        // Query must be sorted. URL.query doesn't guarantee this but for simple tests we manage it.
        // Let's assume input query is already encoded or simple.
        // For strictness we should parse and sort.
        var canonicalQuery = ""
        if !query.isEmpty {
            let items = query.split(separator: "&").map { item -> String in
                if !item.contains("=") {
                    return "\(item)="
                }
                return String(item)
            }.sorted()
            canonicalQuery = items.joined(separator: "&")
        }

        let canonicalRequest = """
            \(methodStr)
            \(path)
            \(canonicalQuery)
            \(canonicalHeaders)
            \(signedHeaders)
            \(payloadHash)
            """

        // 2. String to Sign
        let algorithm = "AWS4-HMAC-SHA256"
        let credentialScope = "\(dateStamp)/\(region)/\(service)/aws4_request"
        let canonicalRequestHash = SHA256.hash(data: Data(canonicalRequest.utf8)).map {
            String(format: "%02x", $0)
        }.joined()

        let stringToSign = """
            \(algorithm)
            \(amzDate)
            \(credentialScope)
            \(canonicalRequestHash)
            """

        // 3. Signature
        let kSecret = "AWS4" + secretKey
        let kDate = try hmac(key: Data(kSecret.utf8), data: Data(dateStamp.utf8))
        let kRegion = try hmac(key: kDate, data: Data(region.utf8))
        let kService = try hmac(key: kRegion, data: Data(service.utf8))
        let kSigning = try hmac(key: kService, data: Data("aws4_request".utf8))

        let signatureData = try hmac(key: kSigning, data: Data(stringToSign.utf8))
        let signature = signatureData.map { String(format: "%02x", $0) }.joined()

        let authorization =
            "\(algorithm) Credential=\(accessKey)/\(credentialScope), SignedHeaders=\(signedHeaders), Signature=\(signature)"

        var fields = HTTPFields()
        fields[.authorization] = authorization
        fields[.init("x-amz-date")!] = amzDate
        fields[.init("x-amz-content-sha256")!] = payloadHash
        fields[.init("Host")!] = hostHeader  // Hummingbird/NIO might set this auto but we need it for signing match

        return fields
    }

    private func hmac(key: Data, data: Data) throws -> Data {
        let auth = HMAC<SHA256>.authenticationCode(for: data, using: SymmetricKey(data: key))
        return Data(auth)
    }
}

/// Global test helper to set up a SwiftS3 application for testing
func withApp(
    users: [(accessKey: String, secretKey: String)] = [],
    _ test: @escaping @Sendable (any TestClientProtocol, SQLMetadataStore) async throws -> Void
) async throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let threadPool = NIOThreadPool(numberOfThreads: 2)
    threadPool.start()

    let storagePath = FileManager.default.temporaryDirectory.appendingPathComponent(
        UUID().uuidString
    ).path
    try? FileManager.default.createDirectory(atPath: storagePath, withIntermediateDirectories: true)

    let metadataStore = try await SQLMetadataStore.create(
        path: storagePath + "/metadata.sqlite",
        on: elg,
        threadPool: threadPool
    )

    // Seed users
    for user in users {
        try await metadataStore.createUser(
            username: "User-\(user.accessKey)", accessKey: user.accessKey, secretKey: user.secretKey
        )
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
        eventLoopGroupProvider: .shared(elg)
    )

    try await app.test(.router) { client in
        try await test(client, metadataStore)
    }

    try? await storage.shutdown()
    try? await metadataStore.shutdown()
    try? await threadPool.shutdownGracefully()
    try? await elg.shutdownGracefully()
    try? FileManager.default.removeItem(atPath: storagePath)
}

/// Global test helper to set up a SwiftS3 application with mock storage for fast unit testing
func withMockApp(
    _ test: @escaping @Sendable (any TestClientProtocol, MockStorage) async throws -> Void
) async throws {
    let mockStorage = MockStorage()
    let controller = S3Controller(storage: mockStorage)

    let router = Router(context: S3RequestContext.self)
    router.middlewares.add(S3ErrorMiddleware())
    router.middlewares.add(MockAuthenticatorMiddleware())
    controller.addRoutes(to: router)

    let app = Application(
        router: router,
        configuration: .init(address: .hostname("127.0.0.1", port: 0))
    )

    try await app.test(.router) { client in
        try await test(client, mockStorage)
    }
}
