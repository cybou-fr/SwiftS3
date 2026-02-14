import Foundation

/// User store that supports both local users and LDAP authentication
struct LDAPUserStore: UserStore {
    let localStore: UserStore
    let ldapConfig: LDAPConfig?

    init(localStore: UserStore, ldapConfig: LDAPConfig?) {
        self.localStore = localStore
        self.ldapConfig = ldapConfig
    }

    func createUser(username: String, accessKey: String, secretKey: String) async throws {
        try await localStore.createUser(username: username, accessKey: accessKey, secretKey: secretKey)
    }

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

    func listUsers() async throws -> [User] {
        try await localStore.listUsers()
    }

    func deleteUser(accessKey: String) async throws {
        try await localStore.deleteUser(accessKey: accessKey)
    }

    /// Authenticate user via LDAP
    /// For demo purposes, this is a mock implementation
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