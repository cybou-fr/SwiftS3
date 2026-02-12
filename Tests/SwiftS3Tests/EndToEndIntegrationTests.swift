import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting
import NIO
import Testing

@testable import SwiftS3

@Suite("End-to-End Integration Tests")
struct EndToEndIntegrationTests {

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
            secretKey: "password")

        // Ensure storage directory exists
        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        // Initialize SQL Metadata Store
        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite",
            on: Self.elg,
            threadPool: Self.threadPool
        )

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore)
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

        // Cleanup
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

    @Test("Full CRUD Object Lifecycle")
    func testFullObjectLifecycle() async throws {
        try await withApp { client in
            let bucket = "test-bucket"
            let key = "test-object.txt"
            let content = "Hello, SwiftS3!"

            // 1. Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // 2. Put object
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                body: .string(content)
            ) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.eTag] != nil)
            }

            // 3. Head object (check metadata)
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .head,
                headers: sign("HEAD", "/\(bucket)/\(key)")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.headers[.contentLength] == "14")
                #expect(response.headers[.eTag] != nil)
            }

            // 4. Get object
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .get,
                headers: sign("GET", "/\(bucket)/\(key)")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.body == ByteBuffer(string: content))
            }

            // 5. List objects
            try await client.execute(
                uri: "/\(bucket)",
                method: .get,
                headers: sign("GET", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                #expect(bodyString.contains(key))
            }

            // 6. Copy object
            let copyKey = "copied-object.txt"
            try await client.execute(
                uri: "/\(bucket)/\(copyKey)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(copyKey)") + [
                    HTTPField.Name("x-amz-copy-source")!: "/\(bucket)/\(key)"
                ]
            ) { response in
                #expect(response.status == .ok)
            }

            // 7. Delete object
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .delete,
                headers: sign("DELETE", "/\(bucket)/\(key)")
            ) { response in
                #expect(response.status == .noContent)
            }

            // 8. Verify deletion
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .get,
                headers: sign("GET", "/\(bucket)/\(key)")
            ) { response in
                #expect(response.status == .notFound)
            }

            // 9. Delete bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .delete,
                headers: sign("DELETE", "/\(bucket)")
            ) { response in
                #expect(response.status == .noContent)
            }
        }
    }

    @Test("Multipart Upload End-to-End")
    func testMultipartUpload() async throws {
        try await withApp { client in
            let bucket = "multipart-bucket"
            let key = "multipart-object.txt"

            // 1. Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // 2. Initiate multipart upload
            try await client.execute(
                uri: "/\(bucket)/\(key)?uploads",
                method: .post,
                headers: sign("POST", "/\(bucket)/\(key)?uploads")
            ) { response in
                #expect(response.status == .ok)
                // Parse upload ID from response
            }

            // Note: Full multipart implementation would require parsing XML responses
            // This is a placeholder for the complete flow
        }
    }

    @Test("Bucket Operations with Policies")
    func testBucketPolicyOperations() async throws {
        try await withApp { client in
            let bucket = "policy-bucket"

            // 1. Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // 2. Put bucket policy
            let policy = """
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::\(bucket)/*"
                    }
                ]
            }
            """

            try await client.execute(
                uri: "/\(bucket)?policy",
                method: .put,
                headers: sign("PUT", "/\(bucket)?policy", body: policy),
                body: .string(policy)
            ) { response in
                #expect(response.status == .noContent)
            }

            // 3. Get bucket policy
            try await client.execute(
                uri: "/\(bucket)?policy",
                method: .get,
                headers: sign("GET", "/\(bucket)?policy")
            ) { response in
                #expect(response.status == .ok)
                #expect(response.body == ByteBuffer(string: policy))
            }

            // 4. Delete bucket policy
            try await client.execute(
                uri: "/\(bucket)?policy",
                method: .delete,
                headers: sign("DELETE", "/\(bucket)?policy")
            ) { response in
                #expect(response.status == .noContent)
            }
        }
    }

    @Test("Tagging Operations")
    func testObjectTagging() async throws {
        try await withApp { client in
            let bucket = "tag-bucket"
            let key = "tagged-object.txt"
            let content = "Tagged content"

            // 1. Create bucket and object
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(key)", body: content),
                body: .string(content)
            ) { response in
                #expect(response.status == .ok)
            }

            // 2. Put object tags
            let tagsXML = """
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>Environment</Key>
                        <Value>Production</Value>
                    </Tag>
                    <Tag>
                        <Key>Owner</Key>
                        <Value>TeamA</Value>
                    </Tag>
                </TagSet>
            </Tagging>
            """

            try await client.execute(
                uri: "/\(bucket)/\(key)?tagging",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(key)?tagging", body: tagsXML),
                body: .string(tagsXML)
            ) { response in
                #expect(response.status == .ok)
            }

            // 3. Get object tags
            try await client.execute(
                uri: "/\(bucket)/\(key)?tagging",
                method: .get,
                headers: sign("GET", "/\(bucket)/\(key)?tagging")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                #expect(bodyString.contains("Environment"))
                #expect(bodyString.contains("Production"))
            }
        }
    }

    @Test("Versioning Operations")
    func testVersioningOperations() async throws {
        try await withApp { client in
            let bucket = "versioned-bucket"
            let key = "versioned-object.txt"

            // 1. Create bucket
            try await client.execute(
                uri: "/\(bucket)",
                method: .put,
                headers: sign("PUT", "/\(bucket)")
            ) { response in
                #expect(response.status == .ok)
            }

            // 2. Enable versioning
            let versioningXML = """
            <VersioningConfiguration>
                <Status>Enabled</Status>
            </VersioningConfiguration>
            """

            try await client.execute(
                uri: "/\(bucket)?versioning",
                method: .put,
                headers: sign("PUT", "/\(bucket)?versioning", body: versioningXML),
                body: .string(versioningXML)
            ) { response in
                #expect(response.status == .ok)
            }

            // 3. Put object (creates version 1)
            let content1 = "Version 1"
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(key)", body: content1),
                body: .string(content1)
            ) { response in
                #expect(response.status == .ok)
            }

            // 4. Put object again (creates version 2)
            let content2 = "Version 2"
            try await client.execute(
                uri: "/\(bucket)/\(key)",
                method: .put,
                headers: sign("PUT", "/\(bucket)/\(key)", body: content2),
                body: .string(content2)
            ) { response in
                #expect(response.status == .ok)
            }

            // 5. List versions
            try await client.execute(
                uri: "/\(bucket)?versions",
                method: .get,
                headers: sign("GET", "/\(bucket)?versions")
            ) { response in
                #expect(response.status == .ok)
                let bodyString = String(buffer: response.body)
                #expect(bodyString.contains(key))
            }
        }
    }
}</content>
<parameter name="filePath">/Users/cybou/Documents/SwiftS3/Tests/SwiftS3Tests/EndToEndIntegrationTests.swift