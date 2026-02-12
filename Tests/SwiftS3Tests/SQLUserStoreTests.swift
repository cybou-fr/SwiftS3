import NIO
import SQLiteNIO
import XCTest

@testable import SwiftS3

final class SQLUserStoreTests: XCTestCase {

    // Shared resources
    static let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    static let threadPool: NIOThreadPool = {
        let tp = NIOThreadPool(numberOfThreads: 2)
        tp.start()
        return tp
    }()

    override class func tearDown() {
        try? threadPool.syncShutdownGracefully()
        try? elg.syncShutdownGracefully()
    }

    // MARK: - Helper
    func withUserStore(_ test: @escaping (UserStore) async throws -> Void) async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            .path
        try? FileManager.default.createDirectory(atPath: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: root) }

        var metadataStore: SQLMetadataStore?
        do {
            metadataStore = try await SQLMetadataStore.create(
                path: root + "/metadata.sqlite",
                on: SQLUserStoreTests.elg,
                threadPool: SQLUserStoreTests.threadPool
            )

            if let store = metadataStore {
                try await test(store)
                try await store.shutdown()
            }
        } catch {
            try? await metadataStore?.shutdown()
            throw error
        }
    }

    // MARK: - Tests

    func testSeedingAndList() async throws {
        try await withUserStore { store in
            let users = try await store.listUsers()
            XCTAssertEqual(users.count, 1)
            XCTAssertEqual(users.first?.username, "admin")
            XCTAssertEqual(users.first?.accessKey, "admin")

            // Check get user
            let admin = try await store.getUser(accessKey: "admin")
            XCTAssertNotNil(admin)
            XCTAssertEqual(admin?.secretKey, "password")
        }
    }

    func testCreateUser() async throws {
        try await withUserStore { store in
            try await store.createUser(
                username: "alice", accessKey: "aliceKey", secretKey: "aliceSecret")

            let users = try await store.listUsers()
            XCTAssertEqual(users.count, 2)  // admin + alice

            let alice = try await store.getUser(accessKey: "aliceKey")
            XCTAssertEqual(alice?.username, "alice")
            XCTAssertEqual(alice?.secretKey, "aliceSecret")
        }
    }

    func testDeleteUser() async throws {
        try await withUserStore { store in
            // Delete admin
            try await store.deleteUser(accessKey: "admin")

            let users = try await store.listUsers()
            XCTAssertEqual(users.count, 0)

            let start = try await store.getUser(accessKey: "admin")
            XCTAssertNil(start)
        }
    }
}
