import ArgumentParser
import Hummingbird
import Foundation

@main
struct SwiftS3: AsyncParsableCommand {
    @Option(name: .shortAndLong, help: "Port to bind to")
    var port: Int = 8080
    
    @Option(name: .shortAndLong, help: "Hostname to bind to")
    var hostname: String = "127.0.0.1"
    
    @Option(name: .shortAndLong, help: "Storage directory path")
    var storage: String = "./data"
    
    func run() async throws {
        // Ensure storage directory exists
        let fileManager = FileManager.default
        if !fileManager.fileExists(atPath: storage) {
            try fileManager.createDirectory(atPath: storage, withIntermediateDirectories: true)
        }
        
        let server = S3Server(hostname: hostname, port: port, storagePath: storage)
        print("Starting SwiftS3 server on \(hostname):\(port) with storage at \(storage)")
        try await server.run()
    }
}
