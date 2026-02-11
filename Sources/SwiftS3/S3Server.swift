import ArgumentParser
import Foundation
import Hummingbird
import NIO

struct S3Server {
    let hostname: String
    let port: Int
    let storagePath: String
    let accessKey: String
    let secretKey: String

    func run() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let threadPool = NIOThreadPool(numberOfThreads: 2)
        threadPool.start()
        
        let metadataStore = try await SQLMetadataStore.create(
            path: storagePath + "/metadata.sqlite", 
            on: elg,
            threadPool: threadPool
        )
        // Ensure storage directory exists for sqlite file
        try? FileManager.default.createDirectory(atPath: storagePath, withIntermediateDirectories: true)
        
        let storage = FileSystemStorage(rootPath: storagePath, metadataStore: metadataStore)
        let controller = S3Controller(storage: storage)

        let router = Router()
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(accessKey: accessKey, secretKey: secretKey))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(hostname, port: port)),
            eventLoopGroupProvider: .shared(elg) 
        )

        try await app.runService()
        
        try await metadataStore.shutdown()
        try await metadataStore.shutdown()
        try await threadPool.shutdownGracefully()
        try await elg.shutdownGracefully()
    }
}

extension NIOThreadPool {
    func shutdownGracefully() async throws {
        return try await withCheckedThrowingContinuation { continuation in
            self.shutdownGracefully { error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }
}
