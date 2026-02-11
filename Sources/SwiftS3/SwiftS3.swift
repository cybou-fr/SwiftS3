import ArgumentParser
import Foundation
import Hummingbird
import Logging

@main
struct SwiftS3: AsyncParsableCommand {
    @Option(name: .shortAndLong, help: "Port to bind to")
    var port: Int = 8080

    @Option(name: .shortAndLong, help: "Hostname to bind to")
    var hostname: String = "127.0.0.1"

    @Option(name: .shortAndLong, help: "Storage directory path")
    var storage: String = "./data"

    @Option(
        name: .customLong("access-key"),
        help: "AWS Access Key ID (fallback to AWS_ACCESS_KEY_ID env var)")
    var accessKey: String?

    @Option(
        name: .customLong("secret-key"),
        help: "AWS Secret Access Key (fallback to AWS_SECRET_ACCESS_KEY env var)")
    var secretKey: String?

    func run() async throws {
        // Resolve credentials
        let processInfo = ProcessInfo.processInfo
        let finalAccessKey = accessKey ?? processInfo.environment["AWS_ACCESS_KEY_ID"] ?? "admin"
        let finalSecretKey =
            secretKey ?? processInfo.environment["AWS_SECRET_ACCESS_KEY"] ?? "password"

        // Ensure storage directory exists
        let fileManager = FileManager.default
        if !fileManager.fileExists(atPath: storage) {
            try fileManager.createDirectory(atPath: storage, withIntermediateDirectories: true)
        }

        let server = S3Server(
            hostname: hostname, port: port, storagePath: storage,
            accessKey: finalAccessKey, secretKey: finalSecretKey)
        try await server.run()
    }
}
