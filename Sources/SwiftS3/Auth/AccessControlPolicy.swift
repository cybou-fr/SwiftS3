import Foundation

/// Represents an AWS S3 Access Control List (ACL) policy.
/// Defines permissions for principals (users/groups) to perform actions on buckets and objects.
/// ACLs provide basic access control with predefined permission sets (read, write, full-control).
public struct AccessControlPolicy: Codable, Sendable {
    public let owner: Owner
    public let accessControlList: [Grant]

    public init(owner: Owner, accessControlList: [Grant]) {
        self.owner = owner
        self.accessControlList = accessControlList
    }
}

/// Represents the owner of a bucket or object in S3.
/// Contains the canonical user ID and optional display name.
/// The owner always has implicit full control over their resources.
public struct Owner: Codable, Sendable {
    public let id: String
    public let displayName: String?

    public init(id: String, displayName: String? = nil) {
        self.id = id
        self.displayName = displayName
    }
}

/// Represents a single permission grant in an ACL.
/// Defines what permission a specific grantee (user/group) has on a resource.
/// Multiple grants can exist for different grantees and permissions.
public struct Grant: Codable, Sendable {
    public let grantee: Grantee
    public let permission: Permission

    public init(grantee: Grantee, permission: Permission) {
        self.grantee = grantee
        self.permission = permission
    }
}

/// Represents the recipient of an ACL permission grant.
/// Can be a specific user (by ID), a predefined group, or all users.
/// Supports both canonical user IDs and URI-based group identifiers.
public struct Grantee: Codable, Sendable {
    public let id: String?
    public let displayName: String?
    public let type: String  // e.g. "CanonicalUser" or "Group"
    public let uri: String?  // For Groups

    public init(
        id: String? = nil, displayName: String? = nil, type: String = "CanonicalUser",
        uri: String? = nil
    ) {
        self.id = id
        self.displayName = displayName
        self.type = type
        self.uri = uri
    }

    /// Creates a group grantee with the specified URI
    /// - Parameter uri: The URI identifying the group (e.g., AllUsers or AuthenticatedUsers)
    /// - Returns: A Grantee instance representing the group
    public static func group(uri: String) -> Grantee {
        return Grantee(type: "Group", uri: uri)
    }

    /// Creates a user grantee with the specified ID and optional display name
    /// - Parameters:
    ///   - id: The canonical user ID
    ///   - displayName: Optional display name for the user
    /// - Returns: A Grantee instance representing the user
    public static func user(id: String, displayName: String? = nil) -> Grantee {
        return Grantee(id: id, displayName: displayName, type: "CanonicalUser")
    }
}

/// Defines the standard S3 ACL permissions.
/// Maps to AWS S3 permission strings used in ACL grants.
/// Permissions can be combined in grants to provide specific access levels.
public enum Permission: String, Codable, Sendable {
    case read = "READ"
    case write = "WRITE"
    case readAcp = "READ_ACP"
    case writeAcp = "WRITE_ACP"
    case fullControl = "FULL_CONTROL"
}

/// Predefined ACL configurations for common use cases.
/// Provides convenient shortcuts for setting up standard permission sets.
/// Each canned ACL translates to a specific set of grants on the resource.
public enum CannedACL: String, Sendable {
    case privateACL = "private"
    case publicRead = "public-read"
    case publicReadWrite = "public-read-write"
    case authenticatedRead = "authenticated-read"

    /// Creates an AccessControlPolicy with the canned ACL configuration
    /// - Parameter owner: The owner of the resource who will receive full control permissions
    /// - Returns: An AccessControlPolicy with appropriate grants based on the canned ACL type
    /// - Note: Owner always receives FULL_CONTROL permission regardless of canned ACL type
    public func createPolicy(owner: Owner) -> AccessControlPolicy {
        var grants: [Grant] = []

        // Owner always gets FULL_CONTROL (usually) - verified against AWS behavior:
        // "private": Owner gets FULL_CONTROL. No one else has access rights.
        grants.append(
            Grant(
                grantee: .user(id: owner.id, displayName: owner.displayName),
                permission: .fullControl))

        switch self {
        case .privateACL:
            break
        case .publicRead:
            // AllUsers group gets READ
            grants.append(
                Grant(
                    grantee: .group(uri: "http://acs.amazonaws.com/groups/global/AllUsers"),
                    permission: .read))
        case .publicReadWrite:
            // AllUsers group gets READ and WRITE
            grants.append(
                Grant(
                    grantee: .group(uri: "http://acs.amazonaws.com/groups/global/AllUsers"),
                    permission: .read))
            grants.append(
                Grant(
                    grantee: .group(uri: "http://acs.amazonaws.com/groups/global/AllUsers"),
                    permission: .write))
        case .authenticatedRead:
            // AuthenticatedUsers group gets READ
            grants.append(
                Grant(
                    grantee: .group(
                        uri: "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"),
                    permission: .read))
        }

        return AccessControlPolicy(owner: owner, accessControlList: grants)
    }
}
