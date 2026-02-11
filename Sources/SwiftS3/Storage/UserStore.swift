import Foundation

/// Model representing a user in the system
public struct User: Codable, Sendable {
    public let username: String
    public let accessKey: String
    public let secretKey: String
}

/// Protocol defining user management operations
public protocol UserStore: Sendable {
    /// Create a new user with credentials
    func createUser(username: String, accessKey: String, secretKey: String) async throws

    /// Retrieve a user by their Access Key (used for authentication)
    func getUser(accessKey: String) async throws -> User?

    /// List all registered users
    func listUsers() async throws -> [User]

    /// Delete a user by Access Key
    func deleteUser(accessKey: String) async throws
}
