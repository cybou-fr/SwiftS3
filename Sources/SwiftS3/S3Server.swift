import ArgumentParser
import Foundation
import Hummingbird
import Logging
import NIO

/// LDAP configuration for enterprise authentication
struct LDAPConfig {
    let server: String
    let baseDN: String
    let bindDN: String
    let bindPassword: String
}

/// Configuration and initialization for the SwiftS3 server.
struct S3Server {
    let hostname: String
    let port: Int
    let storagePath: String
    let accessKey: String
    let secretKey: String
    let ldapConfig: LDAPConfig?

    /// Starts the S3 server with the configured settings.
    func run() async throws {
        let logger = Logger(label: "SwiftS3")

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()

        logger.info(
            "Initializing SQLite metadata store",
            metadata: ["path": "\(storagePath)/metadata.sqlite"])
        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite",
            on: elg,
            threadPool: threadPool
        )
        // Ensure storage directory exists for sqlite file
        try? FileManager.default.createDirectory(
            atPath: storagePath, withIntermediateDirectories: true)

        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore)
        let userStore = LDAPUserStore(localStore: metadataStore, ldapConfig: ldapConfig)
        let controller = S3Controller(storage: storage)

        // Start Lifecycle Janitor
        let janitor = LifecycleJanitor(storage: storage, interval: .seconds(60))  // Check every 60 seconds for demo/testing
        await janitor.start()

        let router = Router(context: S3RequestContext.self)
        router.middlewares.add(S3RequestLogger())
        router.middlewares.add(S3MetricsMiddleware(metrics: controller.metrics))
        router.middlewares.add(S3ErrorMiddleware())
        
        // Add metrics endpoint before authentication
        router.get("/metrics") { request, context in
            let metricsOutput = await controller.metrics.getMetrics()
            return Response(status: .ok, headers: [.contentType: "text/plain"], body: .init(byteBuffer: ByteBuffer(string: metricsOutput)))
        }
        
        router.middlewares.add(S3Authenticator(userStore: userStore))
        router.middlewares.add(S3VpcMiddleware(storage: storage))
        router.middlewares.add(S3AuditMiddleware(storage: storage))
        controller.addRoutes(to: router)

        logger.info(
            "Starting server",
            metadata: ["hostname": "\(hostname)", "port": "\(port)", "storage": "\(storagePath)"])

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(hostname, port: port)),
            eventLoopGroupProvider: .shared(elg)
        )

        try await app.runService()

        logger.info("Shutting down")
        await janitor.stop()
        try await metadataStore.shutdown()
        try await threadPool.shutdownGracefully()
        try await elg.shutdownGracefully()
    }
}
