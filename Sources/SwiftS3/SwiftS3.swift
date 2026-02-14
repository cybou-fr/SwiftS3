import ArgumentParser
import Foundation
import Hummingbird
import Logging
import NIO
import SQLiteNIO

/// Main command-line interface for SwiftS3 Object Storage Server.
/// Provides commands to start the server and manage users.
@main
struct SwiftS3: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "SwiftS3 Object Storage Server",
        subcommands: [ServerCommand.self, UserCommand.self],
        defaultSubcommand: ServerCommand.self
    )
}

/// Command to start the S3 server.
struct ServerCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(commandName: "server")

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

    @Option(name: .customLong("ldap-server"), help: "LDAP server URL (e.g., ldap://localhost:389)")
    var ldapServer: String?

    @Option(name: .customLong("ldap-base-dn"), help: "LDAP base DN for user searches")
    var ldapBaseDN: String?

    @Option(name: .customLong("ldap-bind-dn"), help: "LDAP bind DN for authentication")
    var ldapBindDN: String?

    @Option(name: .customLong("ldap-bind-password"), help: "LDAP bind password")
    var ldapBindPassword: String?

    /// Starts the SwiftS3 server with the configured options.
    /// Initializes storage, metadata store, and starts the HTTP server.
    /// Runs until interrupted or an error occurs.
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
            accessKey: finalAccessKey, secretKey: finalSecretKey,
            ldapConfig: ldapServer.map { server in
                LDAPConfig(
                    server: server,
                    baseDN: ldapBaseDN ?? "",
                    bindDN: ldapBindDN ?? "",
                    bindPassword: ldapBindPassword ?? ""
                )
            }
        )
        try await server.run()
    }
}

/// Command to manage users in the S3 server.
struct UserCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "user",
        abstract: "Manage users",
        subcommands: [Create.self, List.self, Delete.self]
    )
}

extension UserCommand {
    struct Create: AsyncParsableCommand {
        @Argument(help: "Username")
        var username: String

        @Option(name: .customLong("access-key"), help: "Access Key")
        var accessKey: String

        @Option(name: .customLong("secret-key"), help: "Secret Key")
        var secretKey: String

        @Option(name: .shortAndLong, help: "Storage directory path")
        var storage: String = "./data"

        /// Creates a new user account with the specified credentials.
        func run() async throws {
            try await withUserStore(path: storage) { store in
                try await store.createUser(
                    username: username, accessKey: accessKey, secretKey: secretKey)
                print("User '\(username)' created successfully.")
            }
        }
    }

    struct List: AsyncParsableCommand {
        @Option(name: .shortAndLong, help: "Storage directory path")
        var storage: String = "./data"

        /// Lists all registered user accounts.
        func run() async throws {
            try await withUserStore(path: storage) { store in
                let users = try await store.listUsers()
                print("Registered Users:")
                print("USERNAME             ACCESS KEY          ")
                print(String(repeating: "-", count: 45))
                for user in users {
                    let u = user.username.padding(toLength: 20, withPad: " ", startingAt: 0)
                    let k = user.accessKey.padding(toLength: 20, withPad: " ", startingAt: 0)
                    print("\(u) \(k)")
                }
            }
        }
    }

    struct Delete: AsyncParsableCommand {
        @Argument(help: "Access Key of the user to delete")
        var accessKey: String

        @Option(name: .shortAndLong, help: "Storage directory path")
        var storage: String = "./data"

        /// Deletes a user account by access key.
        func run() async throws {
            try await withUserStore(path: storage) { store in
                // Check if exists?
                if (try await store.getUser(accessKey: accessKey)) != nil {
                    try await store.deleteUser(accessKey: accessKey)
                    print("User with access key '\(accessKey)' deleted.")
                } else {
                    print("Error: User with access key '\(accessKey)' not found.")
                    throw ExitCode.failure
                }
            }
        }
    }
}

// Helper to initialize store for CLI commands
/// Creates and configures the necessary infrastructure for user management operations.
/// Sets up NIO event loop group and thread pool for async database operations.
/// Ensures proper cleanup of resources after operation completion.
///
/// - Parameters:
///   - path: File system path to the storage directory
///   - operation: Async closure that performs the user management operation
/// - Throws: Any error from the operation or resource initialization
func withUserStore(path: String, operation: (UserStore) async throws -> Void) async throws {
    let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let threadPool = NIOThreadPool(numberOfThreads: 1)
    threadPool.start()

    // Ensure directory exists
    if !FileManager.default.fileExists(atPath: path) {
        try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
    }

    let store = try await SQLMetadataStore.create(
        path: path + "/metadata.sqlite",
        on: elg,
        threadPool: threadPool
    )

    do {
        try await operation(store)
    } catch {
        print("Error executing operation: \(error)")
        // Continue cleanup
    }

    try await store.shutdown()
    try await threadPool.shutdownGracefully()
    try await elg.shutdownGracefully()
}
