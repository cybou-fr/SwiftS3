import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("Edge Case and Boundary Condition Tests")
struct EdgeCaseTests {

    static let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    static let threadPool: NIOThreadPool = {
        let tp = NIOThreadPool(numberOfThreads: 2)
        tp.start()
        return tp
    }()

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

    @Test("Empty Object Operations")
    func testEmptyObjectOperations() async throws {
        try await withApp { client in
            let bucket = "empty-objects-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Put empty object
            try await client.execute(
                uri: "/\(bucket)/empty.txt",
                method: .put,
                headers: sign("PUT", "/\(bucket)/empty.txt", body: ""),
                body: ByteBuffer(string: "")
            ) { response in
                #expect(response.status == .ok)
            }

            // Get empty object
            try await client.execute(
                uri: "/\(bucket)/empty.txt",
                method: .get,
                headers: sign("GET", "/\(bucket)/empty.txt")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.body.readableBytes == 0)
            }

            // Head empty object
            try await client.execute(
                uri: "/\(bucket)/empty.txt",
                method: .head,
                headers: sign("HEAD", "/\(bucket)/empty.txt")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentLength] == "0")
            }
        }
    }

    @Test("Large Key Names")
    func testLargeKeyNames() async throws {
        try await withApp { client in
            let bucket = "large-keys-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Test with very long key name (1025 characters - exceeds S3 limit)
            let longKey = String(repeating: "a", count: 1025)
            let content = "content"
            var putStatus: HTTPResponse.Status?

            try await client.execute(
                uri: "/\(bucket)/\(longKey)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(longKey)", body: content),
                body: ByteBuffer(string: content)
            ) { response in
                putStatus = response.status
                // Long keys should either succeed or fail gracefully with client error
                #expect(response.status.code >= 200 && response.status.code < 500)
            }

            // Only test retrieval if upload succeeded
            if let putStatus = putStatus, (200...299).contains(putStatus.code) {
                // Retrieve the object
                try await client.execute(
                    uri: "/\(bucket)/\(longKey)",
                    method: .get,
                    headers: sign("GET", "/\(bucket)/\(longKey)")
                ) { response in
                    #expect(response.status == .ok)
                    #expect(String(buffer: response.body) == content)
                }
            }
        }
    }

    @Test("Unicode Key Names")
    func testUnicodeKeyNames() async throws {
        try await withApp { client in
            let bucket = "unicode-keys-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Test with Unicode characters in key
            let unicodeKey = "测试文件-ñáéíóú.txt"
            let content = "unicode content"

            try await client.execute(
                uri: "/\(bucket)/\(unicodeKey)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(unicodeKey)", body: content),
                body: ByteBuffer(string: content)
            ) { response in
                #expect(response.status == .ok)
            }

            // Retrieve the object
            try await client.execute(
                uri: "/\(bucket)/\(unicodeKey)",
                method: .get,
                headers: sign("GET", "/\(bucket)/\(unicodeKey)")
            ) { response in
                #expect(response.status == .ok)
                #expect(String(buffer: response.body) == content)
            }
        }
    }

    @Test("Special Characters in Keys")
    func testSpecialCharactersInKeys() async throws {
        try await withApp { client in
            let bucket = "special-chars-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Test with various special characters
            let specialKeys = [
                "file with spaces.txt",
                "file-with-dashes.txt",
                "file_with_underscores.txt",
                "file.with.dots.txt",
                "file+with+plus.txt",
                "file%with%percent.txt"
            ]

            for key in specialKeys {
                let content = "content for \(key)"

                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .put,
                    headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                    body: ByteBuffer(string: content)
                ) { response in
                    #expect(response.status == .ok)
                }

                // Retrieve and verify
                try await client.execute(
                    uri: "/\(bucket)/\(key)",
                    method: .get,
                    headers: sign("GET", "/\(bucket)/\(key)")
                ) { response in
                    #expect(response.status == .ok)
                    #expect(String(buffer: response.body) == content)
                }
            }
        }
    }

    @Test("Zero-byte Upload")
    func testZeroByteUpload() async throws {
        try await withApp { client in
            let bucket = "zero-byte-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Put zero-byte object
            try await client.execute(
                uri: "/\(bucket)/zero.txt",
                method: .put,
                headers: sign("PUT", "/\(bucket)/zero.txt", body: ""),
                body: ByteBuffer()
            ) { response in
                #expect(response.status == .ok)
            }

            // Get zero-byte object
            try await client.execute(
                uri: "/\(bucket)/zero.txt",
                method: .get,
                headers: sign("GET", "/\(bucket)/zero.txt")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.body.readableBytes == 0)
            }
        }
    }

    @Test("Extremely Long Content")
    func testExtremelyLongContent() async throws {
        try await withApp { client in
            let bucket = "long-content-bucket"

            // Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // Create content with 1MB of data
            let longContent = String(repeating: "x", count: 1024 * 1024)

            try await client.execute(
                uri: "/\(bucket)/large.txt",
                method: .put,
                headers: sign("PUT", "/\(bucket)/large.txt", body: longContent),
                body: ByteBuffer(string: longContent)
            ) { response in
                #expect(response.status == .ok)
            }

            // Get and verify content length
            try await client.execute(
                uri: "/\(bucket)/large.txt",
                method: .get,
                headers: sign("GET", "/\(bucket)/large.txt")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.body.readableBytes == 1024 * 1024)
            }
        }
    }
}