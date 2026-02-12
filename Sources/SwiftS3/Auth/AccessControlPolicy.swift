import Foundation

public struct AccessControlPolicy: Codable, Sendable {
    public let owner: Owner
    public let accessControlList: [Grant]

    public init(owner: Owner, accessControlList: [Grant]) {
        self.owner = owner
        self.accessControlList = accessControlList
    }
}

public struct Owner: Codable, Sendable {
    public let id: String
    public let displayName: String?

    public init(id: String, displayName: String? = nil) {
        self.id = id
        self.displayName = displayName
    }
}

public struct Grant: Codable, Sendable {
    public let grantee: Grantee
    public let permission: Permission

    public init(grantee: Grantee, permission: Permission) {
        self.grantee = grantee
        self.permission = permission
    }
}

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

    public static func group(uri: String) -> Grantee {
        return Grantee(type: "Group", uri: uri)
    }

    public static func user(id: String, displayName: String? = nil) -> Grantee {
        return Grantee(id: id, displayName: displayName, type: "CanonicalUser")
    }
}

public enum Permission: String, Codable, Sendable {
    case read = "READ"
    case write = "WRITE"
    case readAcp = "READ_ACP"
    case writeAcp = "WRITE_ACP"
    case fullControl = "FULL_CONTROL"
}

public enum CannedACL: String, Sendable {
    case privateACL = "private"
    case publicRead = "public-read"
    case publicReadWrite = "public-read-write"
    case authenticatedRead = "authenticated-read"

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
