import Foundation

/// User store that supports both local users and LDAP authentication
struct LDAPUserStore: UserStore {
    let localStore: UserStore
    let ldapConfig: LDAPConfig?

    /// Initializes the LDAP user store with local storage and optional LDAP configuration
    /// - Parameters:
    ///   - localStore: The local user store for fallback authentication
    ///   - ldapConfig: Optional LDAP configuration for LDAP authentication
    init(localStore: UserStore, ldapConfig: LDAPConfig?) {
        self.localStore = localStore
        self.ldapConfig = ldapConfig
    }

    /// Creates a new user in the local user store
    /// - Parameters:
    ///   - username: The username for the new user
    ///   - accessKey: The access key for the new user
    ///   - secretKey: The secret key for the new user
    /// - Throws: UserStore errors if user creation fails
    func createUser(username: String, accessKey: String, secretKey: String) async throws {
        try await localStore.createUser(username: username, accessKey: accessKey, secretKey: secretKey)
    }

    /// Retrieves a user by access key, checking local store first then LDAP if configured
    /// - Parameter accessKey: The access key to search for
    /// - Returns: The user if found, nil otherwise
    /// - Throws: UserStore errors if retrieval fails
    func getUser(accessKey: String) async throws -> User? {
        // First try local store
        if let user = try await localStore.getUser(accessKey: accessKey) {
            return user
        }

        // If LDAP is configured, try LDAP authentication
        if ldapConfig != nil {
            return try await authenticateViaLDAP(accessKey: accessKey)
        }

        return nil
    }

    /// Lists all users from the local user store
    /// - Returns: Array of all users
    /// - Throws: UserStore errors if listing fails
    func listUsers() async throws -> [User] {
        try await localStore.listUsers()
    }

    /// Deletes a user from the local user store
    /// - Parameter accessKey: The access key of the user to delete
    /// - Throws: UserStore errors if deletion fails
    func deleteUser(accessKey: String) async throws {
        try await localStore.deleteUser(accessKey: accessKey)
    }

    /// Authenticates a user via LDAP using their access key
    /// - Parameter accessKey: The access key to authenticate
    /// - Returns: User object if LDAP authentication succeeds, nil otherwise
    /// - Throws: Authentication errors if LDAP communication fails
    /// - Note: Currently a mock implementation for demo purposes
    private func authenticateViaLDAP(accessKey: String) async throws -> User? {
        // TODO: Implement actual LDAP authentication
        // For now, return a mock user if LDAP is configured
        guard ldapConfig != nil else { return nil }

        // Mock: if access key starts with "ldap-", consider it authenticated
        if accessKey.hasPrefix("ldap-") {
            return User(
                username: "ldap-user",
                accessKey: accessKey,
                secretKey: "mock-secret"  // In real LDAP, this would be validated
            )
        }

        return nil
    }
}