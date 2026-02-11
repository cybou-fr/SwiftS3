import ArgumentParser
import Foundation
import Hummingbird

struct S3Server {
    let hostname: String
    let port: Int
    let storagePath: String
    let accessKey: String
    let secretKey: String

    func run() async throws {
        let storage = FileSystemStorage(rootPath: storagePath)
        let controller = S3Controller(storage: storage)

        let router = Router()
        router.middlewares.add(S3ErrorMiddleware())
        router.middlewares.add(S3Authenticator(accessKey: accessKey, secretKey: secretKey))
        controller.addRoutes(to: router)

        let app = Application(
            router: router,
            configuration: .init(address: .hostname(hostname, port: port))
        )

        try await app.runService()
    }
}
