import Foundation
import XCTest

@testable import SwiftS3

final class AccessControlPolicyTests: XCTestCase {

    func testAccessControlPolicyInit() {
        let owner = Owner(id: "ownerId", displayName: "ownerName")
        let grant = Grant(
            grantee: Grantee(id: "granteeId", type: "CanonicalUser"),
            permission: .read
        )
        let policy = AccessControlPolicy(owner: owner, accessControlList: [grant])
        XCTAssertEqual(policy.owner.id, "ownerId")
        XCTAssertEqual(policy.accessControlList.count, 1)
    }

    func testOwnerInit() {
        let owner = Owner(id: "id", displayName: "name")
        XCTAssertEqual(owner.id, "id")
        XCTAssertEqual(owner.displayName, "name")
    }

    func testGrantInit() {
        let grantee = Grantee(id: "id", type: "CanonicalUser")
        let grant = Grant(grantee: grantee, permission: .write)
        XCTAssertEqual(grant.grantee.id, "id")
        XCTAssertEqual(grant.permission, .write)
    }

    func testGranteeInit() {
        let grantee = Grantee(id: "id", displayName: "name", type: "CanonicalUser", uri: nil)
        XCTAssertEqual(grantee.id, "id")
        XCTAssertEqual(grantee.type, "CanonicalUser")
    }

    func testGranteeGroup() {
        let grantee = Grantee.group(uri: "uri")
        XCTAssertEqual(grantee.type, "Group")
        XCTAssertEqual(grantee.uri, "uri")
    }

    func testAccessControlPolicyCodable() throws {
        let policy = AccessControlPolicy(
            owner: Owner(id: "ownerId"),
            accessControlList: [
                Grant(
                    grantee: Grantee(id: "granteeId", type: "CanonicalUser"),
                    permission: .fullControl
                )
            ]
        )
        let encoder = JSONEncoder()
        let data = try encoder.encode(policy)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(AccessControlPolicy.self, from: data)
        XCTAssertEqual(decoded.owner.id, policy.owner.id)
        XCTAssertEqual(decoded.accessControlList.count, policy.accessControlList.count)
    }

    func testPermissionRawValue() {
        XCTAssertEqual(Permission.fullControl.rawValue, "FULL_CONTROL")
        XCTAssertEqual(Permission.read.rawValue, "READ")
        XCTAssertEqual(Permission.write.rawValue, "WRITE")
    }
}