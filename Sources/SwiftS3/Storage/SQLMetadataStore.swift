import Foundation
import Logging
import NIO
import SQLiteNIO

/// Metadata store implementation using SQLite
/// Provides persistent storage for object metadata, bucket information, and access control.
/// Uses SQLite database for efficient querying, indexing, and concurrent access.
/// Handles versioning, ACLs, policies, and lifecycle management metadata.
struct SQLMetadataStore: MetadataStore {
    let connection: SQLiteConnection
    let logger: Logger = Logger(label: "SwiftS3.SQLMetadataStore")

    init(connection: SQLiteConnection) {
        self.connection = connection
    }

    /// Creates a new SQLite metadata store instance.
    /// Initializes database connection and creates all required tables and indexes.
    ///
    /// - Parameters:
    ///   - path: File system path for the SQLite database file
    ///   - eventLoopGroup: NIO event loop group for async operations
    ///   - threadPool: NIO thread pool for database I/O operations
    /// - Returns: Configured SQLMetadataStore instance
    /// - Throws: Database connection or schema initialization errors
    static func create(path: String, on eventLoopGroup: EventLoopGroup, threadPool: NIOThreadPool)
        async throws -> SQLMetadataStore
    {
        let connection = try await SQLiteConnection.open(
            storage: .file(path: path),
            threadPool: threadPool,
            on: eventLoopGroup.next()
        )
        let store = SQLMetadataStore(connection: connection)
        try await store.initializeSchema()
        return store
    }

    /// Initializes the SQLite database schema.
    /// Creates all required tables, indexes, and initial data for SwiftS3 operation.
    /// Safe to call multiple times - uses IF NOT EXISTS clauses.
    ///
    /// Tables created:
    /// - buckets: Bucket metadata and configuration
    /// - objects: Object metadata with versioning support
    /// - users: User accounts and credentials
    /// - policies: Bucket policies for access control
    ///
    /// - Throws: SQLite errors if schema creation fails
    private func initializeSchema() async throws {
        // Create tables if not exist
        let createBuckets = """
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at REAL,
                owner_id TEXT,
                acl TEXT,
                versioning_status TEXT DEFAULT 'SUSPENDED',
                tags TEXT
            );
            """
        _ = try await connection.query(createBuckets)

        let createObjects = """
            CREATE TABLE IF NOT EXISTS objects (
                bucket TEXT,
                key TEXT,
                version_id TEXT,
                is_latest BOOLEAN DEFAULT 1,
                is_delete_marker BOOLEAN DEFAULT 0,
                size INTEGER,
                last_modified REAL,
                etag TEXT,
                content_type TEXT,
                custom_metadata TEXT,
                owner_id TEXT,
                acl TEXT,
                tags TEXT,
                storage_class TEXT DEFAULT 'STANDARD',
                checksum_algorithm TEXT,
                checksum_value TEXT,
                object_lock_mode TEXT,
                object_lock_retain_until REAL,
                object_lock_legal_hold_status TEXT,
                sse_algorithm TEXT,
                sse_kms_key_id TEXT,
                sse_kms_encryption_context TEXT,
                PRIMARY KEY (bucket, key, version_id),
                FOREIGN KEY(bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );
            """
        _ = try await connection.query(createObjects)

        let createLifecycle = """
            CREATE TABLE IF NOT EXISTS bucket_lifecycle (
                bucket_name TEXT PRIMARY KEY,
                configuration TEXT,
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            );
            """
        _ = try await connection.query(createLifecycle)

        let createObjectLock = """
            CREATE TABLE IF NOT EXISTS bucket_object_lock (
                bucket_name TEXT PRIMARY KEY,
                configuration TEXT,
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            );
            """
        _ = try await connection.query(createObjectLock)

        let createReplication = """
            CREATE TABLE IF NOT EXISTS bucket_replication (
                bucket_name TEXT PRIMARY KEY,
                configuration TEXT,
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            );
            """
        _ = try await connection.query(createReplication)

        let createNotification = """
            CREATE TABLE IF NOT EXISTS bucket_notification (
                bucket_name TEXT PRIMARY KEY,
                configuration TEXT,
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            );
            """
        _ = try await connection.query(createNotification)

        let createVpcConfig = """
            CREATE TABLE IF NOT EXISTS bucket_vpc_config (
                bucket_name TEXT PRIMARY KEY,
                vpc_id TEXT,
                allowed_ip_ranges TEXT,
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            );
            """
        _ = try await connection.query(createVpcConfig)

        let createAuditEvents = """
            CREATE TABLE IF NOT EXISTS audit_events (
                id TEXT PRIMARY KEY,
                timestamp REAL,
                event_type TEXT,
                principal TEXT,
                source_ip TEXT,
                user_agent TEXT,
                request_id TEXT,
                bucket TEXT,
                key TEXT,
                operation TEXT,
                status TEXT,
                error_message TEXT,
                additional_data TEXT
            );
            """
        _ = try await connection.query(createAuditEvents)

        let createBatchJobs = """
            CREATE TABLE IF NOT EXISTS batch_jobs (
                id TEXT PRIMARY KEY,
                operation_type TEXT,
                operation_parameters TEXT,
                manifest_location_bucket TEXT,
                manifest_location_key TEXT,
                manifest_location_etag TEXT,
                manifest_spec_format TEXT,
                manifest_spec_fields TEXT,
                priority INTEGER DEFAULT 0,
                role_arn TEXT,
                status TEXT DEFAULT 'Pending',
                created_at REAL,
                completed_at REAL,
                failure_reasons TEXT,
                total_objects INTEGER DEFAULT 0,
                processed_objects INTEGER DEFAULT 0,
                failed_objects INTEGER DEFAULT 0
            );
            """
        _ = try await connection.query(createBatchJobs)

        let createUsers = """
            CREATE TABLE IF NOT EXISTS users (
                access_key TEXT PRIMARY KEY,
                secret_key TEXT NOT NULL,
                username TEXT NOT NULL
            );
            """
        _ = try await connection.query(createUsers)

        // Seed default user if empty
        let countRes = try await connection.query("SELECT count(*) as count FROM users")
        let count = countRes.first?.column("count")?.integer ?? 0
        if count == 0 {
            _ = try await connection.query(
                "INSERT INTO users (access_key, secret_key, username) VALUES (?, ?, ?)",
                [.text("admin"), .text("password"), .text("admin")])
            logger.info("Seeded default admin user")
        }

        try await migrateSchema()
    }

    private func migrateSchema() async throws {
        // 1. Buckets Migration (Simple ADD COLUMN)
        // owner_id
        do {
            _ = try await connection.query("ALTER TABLE buckets ADD COLUMN owner_id TEXT;")
        } catch {}
        // acl
        do {
            _ = try await connection.query("ALTER TABLE buckets ADD COLUMN acl TEXT;")
        } catch {}
        // versioning_status
        do {
            _ = try await connection.query(
                "ALTER TABLE buckets ADD COLUMN versioning_status TEXT DEFAULT 'SUSPENDED';")
        } catch {}
        // tags
        do {
            _ = try await connection.query("ALTER TABLE buckets ADD COLUMN tags TEXT;")
        } catch {}

        // 2. Objects Migration (Complex Recreate for PK change)
        // Check if version_id exists
        var versionIdExists = false
        do {
            _ = try await connection.query("SELECT version_id FROM objects LIMIT 1")
            versionIdExists = true
        } catch {
            versionIdExists = false
        }

        if !versionIdExists {
            logger.info("Migrating objects table to support versioning...")
            // Rename old table
            _ = try await connection.query("ALTER TABLE objects RENAME TO objects_old")

            // Create new table
            let createObjects = """
                CREATE TABLE IF NOT EXISTS objects (
                    bucket TEXT,
                    key TEXT,
                    version_id TEXT,
                    is_latest BOOLEAN DEFAULT 1,
                    is_delete_marker BOOLEAN DEFAULT 0,
                    size INTEGER,
                    last_modified REAL,
                    etag TEXT,
                    content_type TEXT,
                    custom_metadata TEXT,
                    owner_id TEXT,
                    acl TEXT,
                    tags TEXT,
                    PRIMARY KEY (bucket, key, version_id),
                    FOREIGN KEY(bucket) REFERENCES buckets(name) ON DELETE CASCADE
                );
                """
            _ = try await connection.query(createObjects)

            // Copy data
            // We need to know if objects_old has owner_id and acl to construct select correctly
            // Simple check: try to select them
            var hasOwner = false
            var hasACL = false
            do {
                _ = try await connection.query("SELECT owner_id FROM objects_old LIMIT 1")
                hasOwner = true
            } catch {}
            do {
                _ = try await connection.query("SELECT acl FROM objects_old LIMIT 1")
                hasACL = true
            } catch {}

            let ownerField = hasOwner ? "owner_id" : "'admin'"  // Default to admin if missing
            let aclField = hasACL ? "acl" : "NULL"

            let copyQuery = """
                    INSERT INTO objects (bucket, key, size, last_modified, etag, content_type, custom_metadata, owner_id, acl, tags, version_id, is_latest, is_delete_marker)
                    SELECT bucket, key, size, last_modified, etag, content_type, custom_metadata, \(ownerField), \(aclField), NULL, 'null', 1, 0
                    FROM objects_old;
                """
            _ = try await connection.query(copyQuery)

            // Drop old table
            _ = try await connection.query("DROP TABLE objects_old")
            logger.info("Objects table migration completed.")
        }
    }

    /// Deletes metadata for an object from the database.
    /// Handles versioned and non-versioned deletions, updating the latest version marker as needed.
    /// - Parameters:
    ///   - bucket: The bucket containing the object
    ///   - key: The object key
    ///   - versionId: Optional specific version to delete, or nil for latest version
    /// - Throws: Database errors if the deletion fails
    func deleteMetadata(bucket: String, key: String, versionId: String?) async throws {
        // Check if we are deleting the latest version
        let currentMetadata = try? await getMetadata(bucket: bucket, key: key, versionId: versionId)
        let wasLatest = currentMetadata?.isLatest ?? false

        var query = "DELETE FROM objects WHERE bucket = ? AND key = ?"
        var params: [SQLiteData] = [.text(bucket), .text(key)]

        if let versionId = versionId {
            query += " AND version_id = ?"
            params.append(.text(versionId))
        } else {
            query += " AND is_latest = 1"
        }
        _ = try await connection.query(query, params)

        // Restore latest if needed
        if wasLatest {
            let restoreLatest = """
                UPDATE objects SET is_latest = 1 
                WHERE bucket = ? AND key = ? 
                AND version_id = (
                    SELECT version_id FROM objects 
                    WHERE bucket = ? AND key = ? 
                    ORDER BY last_modified DESC LIMIT 1
                )
                """
            _ = try await connection.query(
                restoreLatest, [.text(bucket), .text(key), .text(bucket), .text(key)])
        }
    }

    /// Retrieves metadata for an object from the database.
    /// Returns the specified version or the latest version if no versionId is provided.
    /// - Parameters:
    ///   - bucket: The bucket containing the object
    ///   - key: The object key
    ///   - versionId: Optional specific version to retrieve, or nil for latest version
    /// - Returns: ObjectMetadata containing all object information
    /// - Throws: Database errors or if the object/version doesn't exist
    func getMetadata(bucket: String, key: String, versionId: String?) async throws -> ObjectMetadata
    {
        var query =
            "SELECT size, last_modified, etag, content_type, custom_metadata, owner_id, version_id, is_latest, is_delete_marker, storage_class, checksum_algorithm, checksum_value, object_lock_mode, object_lock_retain_until, object_lock_legal_hold_status, sse_algorithm, sse_kms_key_id, sse_kms_encryption_context FROM objects WHERE bucket = ? AND key = ?"
        var params: [SQLiteData] = [.text(bucket), .text(key)]

        if let versionId = versionId {
            query += " AND version_id = ?"
            params.append(.text(versionId))
        } else {
            // Get Latest
            query += " AND is_latest = 1"
        }

        let rows = try await connection.query(query, params)

        guard let row = rows.first else {
            throw S3Error.noSuchKey
        }

        // Size
        let size = row.column("size")?.integer ?? 0
        let lastModified = Date(timeIntervalSince1970: row.column("last_modified")?.double ?? 0)
        let eTag = row.column("etag")?.string
        let contentType = row.column("content_type")?.string
        let customMetadataJSON = row.column("custom_metadata")?.string ?? "{}"

        let customMetadata =
            (try? JSONDecoder().decode([String: String].self, from: Data(customMetadataJSON.utf8)))
            ?? [:]

        // Extra columns
        let owner = row.column("owner_id")?.string
        let versionId = row.column("version_id")?.string ?? "null"
        let isLatest = (row.column("is_latest")?.integer ?? 1) == 1
        let isDeleteMarker = (row.column("is_delete_marker")?.integer ?? 0) == 1

        // New fields
        let storageClass = StorageClass(rawValue: row.column("storage_class")?.string ?? "STANDARD") ?? .standard
        let checksumAlgorithm = row.column("checksum_algorithm")?.string.flatMap { ChecksumAlgorithm(rawValue: $0) }
        let checksumValue = row.column("checksum_value")?.string
        let objectLockMode = row.column("object_lock_mode")?.string.flatMap { ObjectLockMode(rawValue: $0) }
        let objectLockRetainUntilDate = row.column("object_lock_retain_until")?.double.flatMap { Date(timeIntervalSince1970: $0) }
        let objectLockLegalHoldStatus = row.column("object_lock_legal_hold_status")?.string.flatMap { LegalHoldStatus(rawValue: $0) }

        // Server-side encryption
        let serverSideEncryption: ServerSideEncryptionConfig? = {
            guard let algorithmStr = row.column("sse_algorithm")?.string,
                  let algorithm = ServerSideEncryption(rawValue: algorithmStr) else {
                return nil
            }
            return ServerSideEncryptionConfig(
                algorithm: algorithm,
                kmsKeyId: row.column("sse_kms_key_id")?.string,
                kmsEncryptionContext: row.column("sse_kms_encryption_context")?.string
            )
        }()

        return ObjectMetadata(
            key: key,
            size: Int64(size),
            lastModified: lastModified,
            eTag: eTag,
            contentType: contentType,
            customMetadata: customMetadata,
            owner: owner,
            versionId: versionId,
            isLatest: isLatest,
            isDeleteMarker: isDeleteMarker,
            storageClass: storageClass,
            checksumAlgorithm: checksumAlgorithm,
            checksumValue: checksumValue,
            objectLockMode: objectLockMode,
            objectLockRetainUntilDate: objectLockRetainUntilDate,
            objectLockLegalHoldStatus: objectLockLegalHoldStatus,
            serverSideEncryption: serverSideEncryption
        )
    }

    /// Creates a new bucket in the metadata store.
    /// - Parameters:
    ///   - name: The bucket name to create
    ///   - owner: The owner ID for the bucket
    /// - Throws: Database errors if bucket creation fails or bucket already exists
    func createBucket(name: String, owner: String) async throws {
        let query = "INSERT INTO buckets (name, created_at, owner_id) VALUES (?, ?, ?)"
        do {
            _ = try await connection.query(
                query,
                [
                    .text(name),
                    .float(Date().timeIntervalSince1970),
                    .text(owner),
                ])
        } catch {
            throw S3Error.bucketAlreadyExists
        }
    }

    /// Deletes a bucket from the metadata store.
    /// Note: This only removes the bucket metadata, actual object cleanup should be handled separately.
    /// - Parameter name: The bucket name to delete
    /// - Throws: Database errors if the deletion fails
    func deleteBucket(name: String) async throws {
        let query = "DELETE FROM buckets WHERE name = ?"
        _ = try await connection.query(query, [.text(name)])
    }

    /// Saves object metadata to the database.
    /// Handles versioning by updating the latest version marker appropriately.
    /// - Parameters:
    ///   - bucket: The bucket containing the object
    ///   - key: The object key
    ///   - metadata: The complete object metadata to save
    /// - Throws: Database errors if the save operation fails
    func saveMetadata(bucket: String, key: String, metadata: ObjectMetadata) async throws {
        // 1. If this is the latest version, update existing latest to false
        if metadata.isLatest {
            let updateLatest = "UPDATE objects SET is_latest = 0 WHERE bucket = ? AND key = ?"
            _ = try await connection.query(updateLatest, [.text(bucket), .text(key)])
        }

        let query = """
            INSERT INTO objects (bucket, key, size, last_modified, etag, content_type, custom_metadata, owner_id, version_id, is_latest, is_delete_marker, storage_class, checksum_algorithm, checksum_value, object_lock_mode, object_lock_retain_until, object_lock_legal_hold_status, sse_algorithm, sse_kms_key_id, sse_kms_encryption_context)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket, key, version_id) DO UPDATE SET
                size=excluded.size,
                last_modified=excluded.last_modified,
                etag=excluded.etag,
                content_type=excluded.content_type,
                custom_metadata=excluded.custom_metadata,
                owner_id=coalesce(excluded.owner_id, objects.owner_id),
                is_latest=excluded.is_latest,
                is_delete_marker=excluded.is_delete_marker,
                storage_class=excluded.storage_class,
                checksum_algorithm=excluded.checksum_algorithm,
                checksum_value=excluded.checksum_value,
                object_lock_mode=excluded.object_lock_mode,
                object_lock_retain_until=excluded.object_lock_retain_until,
                object_lock_legal_hold_status=excluded.object_lock_legal_hold_status,
                sse_algorithm=excluded.sse_algorithm,
                sse_kms_key_id=excluded.sse_kms_key_id,
                sse_kms_encryption_context=excluded.sse_kms_encryption_context;
            """

        let metaJSON = try JSONEncoder().encode(metadata.customMetadata)
        let metaString = String(data: metaJSON, encoding: .utf8) ?? "{}"

        _ = try await connection.query(
            query,
            [
                .text(bucket),
                .text(key),
                .integer(Int(metadata.size)),
                .float(metadata.lastModified.timeIntervalSince1970),
                metadata.eTag.map { .text($0) } ?? .null,
                metadata.contentType.map { .text($0) } ?? .null,
                .text(metaString),
                metadata.owner.map { .text($0) } ?? .null,
                .text(metadata.versionId),
                .integer(metadata.isLatest ? 1 : 0),
                .integer(metadata.isDeleteMarker ? 1 : 0),
                .text(metadata.storageClass.rawValue),
                metadata.checksumAlgorithm.map { .text($0.rawValue) } ?? .null,
                metadata.checksumValue.map { .text($0) } ?? .null,
                metadata.objectLockMode.map { .text($0.rawValue) } ?? .null,
                metadata.objectLockRetainUntilDate.map { .float($0.timeIntervalSince1970) } ?? .null,
                metadata.objectLockLegalHoldStatus.map { .text($0.rawValue) } ?? .null,
                metadata.serverSideEncryption.map { .text($0.algorithm.rawValue) } ?? .null,
                metadata.serverSideEncryption?.kmsKeyId.map { .text($0) } ?? .null,
                metadata.serverSideEncryption?.kmsEncryptionContext.map { .text($0) } ?? .null,
            ])
    }

    // MARK: - ACLs

    /// Retrieves the Access Control List for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket ACL
    ///   - versionId: Optional version ID for object ACL
    /// - Returns: AccessControlPolicy containing the ACL information
    /// - Throws: Database errors or if the resource doesn't exist
    func getACL(bucket: String, key: String?, versionId: String?) async throws
        -> AccessControlPolicy
    {
        let query: String
        let params: [SQLiteData]

        if let key = key {
            var q = "SELECT acl, owner_id FROM objects WHERE bucket = ? AND key = ?"
            var p: [SQLiteData] = [.text(bucket), .text(key)]
            if let versionId = versionId {
                q += " AND version_id = ?"
                p.append(.text(versionId))
            } else {
                q += " AND is_latest = 1"
            }
            query = q
            params = p
        } else {
            query = "SELECT acl, owner_id FROM buckets WHERE name = ?"
            params = [.text(bucket)]
        }

        let rows = try await connection.query(query, params)
        guard let row = rows.first else {
            // Row not found
            if key != nil {
                throw S3Error.noSuchKey
            } else {
                throw S3Error.noSuchBucket
            }
        }

        if let aclJSON = row.column("acl")?.string,
            let data = aclJSON.data(using: .utf8),
            let acl = try? JSONDecoder().decode(AccessControlPolicy.self, from: data)
        {
            return acl
        }

        // Fallback: Default Private ACL
        // If owner_id is present, grant FULL_CONTROL to owner.
        if let ownerID = row.column("owner_id")?.string {
            return CannedACL.privateACL.createPolicy(
                owner: Owner(id: ownerID, displayName: ownerID))
        }

        // If no owner_id (legacy data?), default to admin
        return CannedACL.privateACL.createPolicy(owner: Owner(id: "admin", displayName: "admin"))

    }

    /// Updates the Access Control List for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket ACL
    ///   - versionId: Optional version ID for object ACL
    ///   - acl: The new AccessControlPolicy to apply
    /// - Throws: Database errors if the update fails
    func putACL(bucket: String, key: String?, versionId: String?, acl: AccessControlPolicy)
        async throws
    {
        let aclData = try JSONEncoder().encode(acl)
        let aclString = String(data: aclData, encoding: .utf8) ?? ""
        let ownerId = acl.owner.id

        if let key = key {
            // Update Object
            var query = "UPDATE objects SET acl = ?, owner_id = ? WHERE bucket = ? AND key = ?"
            var params: [SQLiteData] = [
                .text(aclString), .text(ownerId), .text(bucket), .text(key),
            ]

            if let versionId = versionId {
                query += " AND version_id = ?"
                params.append(.text(versionId))
            } else {
                query += " AND is_latest = 1"
            }
            _ = try await connection.query(query, params)
        } else {
            // Update Bucket
            let query = "UPDATE buckets SET acl = ?, owner_id = ? WHERE name = ?"
            _ = try await connection.query(
                query, [.text(aclString), .text(ownerId), .text(bucket)])
        }
    }

    // MARK: - Versioning

    /// Retrieves the versioning configuration for a bucket.
    /// - Parameter bucket: The bucket name
    /// - Returns: VersioningConfiguration if set, or nil if not configured
    /// - Throws: Database errors or if the bucket doesn't exist
    func getBucketVersioning(bucket: String) async throws -> VersioningConfiguration? {
        let query = "SELECT versioning_status FROM buckets WHERE name = ?"
        let rows = try await connection.query(query, [.text(bucket)])

        guard let row = rows.first else {
            throw S3Error.noSuchBucket
        }

        if let statusStr = row.column("versioning_status")?.string,
            let status = VersioningConfiguration.Status(rawValue: statusStr)
        {
            return VersioningConfiguration(status: status)
        }

        return VersioningConfiguration(status: .suspended)
    }

    /// Sets the versioning configuration for a bucket.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - configuration: The new versioning configuration
    /// - Throws: Database errors if the update fails
    func setBucketVersioning(bucket: String, configuration: VersioningConfiguration) async throws {
        let query = "UPDATE buckets SET versioning_status = ? WHERE name = ?"
        _ = try await connection.query(
            query, [.text(configuration.status.rawValue), .text(bucket)])
    }

    /// Lists objects in a bucket with optional filtering and pagination.
    /// Supports prefix, delimiter, and continuation token for large result sets.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - prefix: Optional prefix filter for object keys
    ///   - delimiter: Optional delimiter for hierarchical listing
    ///   - marker: Optional marker for pagination (V1 style)
    ///   - continuationToken: Optional continuation token for pagination (V2 style)
    ///   - maxKeys: Optional maximum number of results to return
    /// - Returns: ListObjectsResult containing the matching objects and pagination info
    /// - Throws: Database errors if the query fails
    func listObjects(
        bucket: String, prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) async throws -> ListObjectsResult {

        // Basic implementation: fetch all matching prefix, then filter in memory for delimiter (hard to do fully in SQL for 'common prefixes' without GROUP BY logic which is complex for delimiters)
        // OR: fetch all matching prefix and do the grouping in Swift (which we did for FS).
        // Since we want performance, SQL LIKE 'prefix%' avoids full scan.

        var query =
            "SELECT key, size, last_modified, etag, content_type, custom_metadata FROM objects WHERE bucket = ?"
        var params: [SQLiteData] = [.text(bucket)]

        if let prefix = prefix {
            query += " AND key LIKE ?"
            // SQLite LIKE is case insensitive by default for ASCII? We need case sensitive?
            // "PRAGMA case_sensitive_like = 1;" might be needed or GLOB.
            // S3 is case sensitive.
            // GLOB is case sensitive.
            // But GLOB uses * and ? instead of % and _.
            // Let's us GLOB? Or just filter in memory after prefix match.
            // Let's use generic prefix match.
            params.append(.text("\(prefix)%"))
        }

        // Ordering
        query += " ORDER BY key ASC"

        // Limit
        // We can't strictly use LIMIT if we have delimiters because we might collapse many keys into one prefix.
        // But if no delimiter, LIMIT is fine.
        // For now, let's fetch a reasonable chunk (safeguard) or all if delimiter is present to process.
        // Since we are "modernizing", let's try to be smart.
        // If delimiter is present, we need to scan.
        // If no delimiter, we can use SQL LIMIT.

        if delimiter == nil {
            let limit = (maxKeys ?? 1000) + 1  // +1 to check truncation
            query += " LIMIT \(limit)"
        }

        let rows = try await connection.query(query, params)

        var allObjects: [ObjectMetadata] = []
        for row in rows {
            let key = row.column("key")?.string ?? ""
            let size = row.column("size")?.integer ?? 0
            let lastModified = Date(timeIntervalSince1970: row.column("last_modified")?.double ?? 0)
            let eTag = row.column("etag")?.string
            let contentType = row.column("content_type")?.string
            let customMetadataJSON = row.column("custom_metadata")?.string ?? "{}"
            let customMetadata =
                (try? JSONDecoder().decode(
                    [String: String].self, from: Data(customMetadataJSON.utf8))) ?? [:]

            allObjects.append(
                ObjectMetadata(
                    key: key,
                    size: Int64(size),
                    lastModified: lastModified,
                    eTag: eTag,
                    contentType: contentType,
                    customMetadata: customMetadata
                ))
        }

        // Reuse the logic from FileSystemStorage for delimiter processing (it expects sorted list)
        // We can extract that logic to a helper or just duplicate for now.
        // Duplicating for speed of implementation, effectively "Client Side" filtering logic on "Server Side" data.

        return processListObjects(
            allObjects: allObjects,
            prefix: prefix,
            delimiter: delimiter,
            marker: marker,
            continuationToken: continuationToken,
            maxKeys: maxKeys
        )
    }

    // Shared Logic (Copied/Adapted from FileSystemStorage)
    private func processListObjects(
        allObjects: [ObjectMetadata],
        prefix: String?, delimiter: String?, marker: String?,
        continuationToken: String?, maxKeys: Int?
    ) -> ListObjectsResult {
        var objects: [ObjectMetadata] = []
        var commonPrefixes: Set<String> = []
        var truncated = false
        var nextMarker: String? = nil
        var nextContinuationToken: String? = nil

        let limit = maxKeys ?? 1000
        var count = 0
        var lastPrefix: String? = nil

        let startAfter = continuationToken ?? marker

        for obj in allObjects {
            if let startAfter = startAfter, obj.key <= startAfter {
                continue
            }

            if count >= limit {
                truncated = true
                nextMarker = objects.last?.key
                nextContinuationToken = objects.last?.key
                break
            }

            let key = obj.key
            var isCommonPrefix = false
            var currentPrefix = ""

            if let delimiter = delimiter {
                let prefixLen = prefix?.count ?? 0
                // safety check ranges
                if key.count > prefixLen {
                    let searchRange = key.index(key.startIndex, offsetBy: prefixLen)..<key.endIndex
                    if let range = key.range(of: delimiter, range: searchRange) {
                        currentPrefix = String(key[..<range.upperBound])
                        isCommonPrefix = true
                    }
                }
            }

            if isCommonPrefix {
                if currentPrefix != lastPrefix {
                    commonPrefixes.insert(currentPrefix)
                    lastPrefix = currentPrefix
                    count += 1
                }
            } else {
                objects.append(obj)
                count += 1
            }
        }

        return ListObjectsResult(
            objects: objects,
            commonPrefixes: Array(commonPrefixes).sorted(),
            isTruncated: truncated,
            nextMarker: nextMarker,
            nextContinuationToken: nextContinuationToken
        )
    }

    /// Lists all versions of objects in a bucket.
    /// Returns versioned objects including delete markers for comprehensive version history.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - prefix: Optional prefix filter for object keys
    ///   - delimiter: Optional delimiter for hierarchical listing
    ///   - keyMarker: Optional key marker for pagination
    ///   - versionIdMarker: Optional version ID marker for pagination
    ///   - maxKeys: Optional maximum number of results to return
    /// - Returns: ListVersionsResult containing all object versions and pagination info
    /// - Throws: Database errors if the query fails
    func listObjectVersions(
        bucket: String, prefix: String?, delimiter: String?, keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) async throws -> ListVersionsResult {
        var query = """
                SELECT
                    key, size, last_modified, etag, content_type, custom_metadata, owner_id, version_id, is_latest, is_delete_marker
                FROM objects
                WHERE bucket = ?
            """
        var params: [SQLiteData] = [.text(bucket)]

        if let prefix = prefix {
            query += " AND key LIKE ?"
            params.append(.text("\(prefix)%"))
        }

        // Ordering is key for delimiter processing
        query += " ORDER BY key ASC, version_id ASC"

        // If no delimiter, we can LIMIT in SQL.
        // If delimiter is present, we need to scan to group prefixes.
        if delimiter == nil {
            let limit = (maxKeys ?? 1000) + 1
            query += " LIMIT \(limit)"
        }

        let rows = try await connection.query(query, params)

        var allVersions: [ObjectMetadata] = []
        for row in rows {
            let key = row.column("key")?.string ?? ""
            let size = Int64(row.column("size")?.integer ?? 0)
            let lastModified = Date(timeIntervalSince1970: row.column("last_modified")?.double ?? 0)
            let eTag = row.column("etag")?.string
            let contentType = row.column("content_type")?.string
            let owner = row.column("owner_id")?.string
            let versionId = row.column("version_id")?.string ?? "null"
            let isLatest = row.column("is_latest")?.bool ?? false
            let isDeleteMarker = row.column("is_delete_marker")?.bool ?? false

            var customMetadata: [String: String] = [:]
            if let metaJSON = row.column("custom_metadata")?.string,
                let data = metaJSON.data(using: .utf8)
            {
                customMetadata =
                    (try? JSONDecoder().decode([String: String].self, from: data)) ?? [:]
            }

            allVersions.append(
                ObjectMetadata(
                    key: key,
                    size: size,
                    lastModified: lastModified,
                    eTag: eTag,
                    contentType: contentType,
                    customMetadata: customMetadata,
                    owner: owner,
                    versionId: versionId,
                    isLatest: isLatest,
                    isDeleteMarker: isDeleteMarker
                ))
        }

        return processListVersions(
            allVersions: allVersions,
            prefix: prefix,
            delimiter: delimiter,
            keyMarker: keyMarker,
            versionIdMarker: versionIdMarker,
            maxKeys: maxKeys
        )
    }

    private func processListVersions(
        allVersions: [ObjectMetadata],
        prefix: String?, delimiter: String?, keyMarker: String?, versionIdMarker: String?,
        maxKeys: Int?
    ) -> ListVersionsResult {
        var versions: [ObjectMetadata] = []
        var commonPrefixes: Set<String> = []
        var truncated = false
        var lastPrefix: String? = nil

        let limit = maxKeys ?? 1000
        var count = 0

        for ver in allVersions {
            // Marker check
            if let keyMarker = keyMarker {
                if ver.key < keyMarker { continue }
                if ver.key == keyMarker {
                    if let vidMarker = versionIdMarker, ver.versionId <= vidMarker {
                        continue
                    }
                }
            }

            if count >= limit {
                truncated = true
                break
            }

            let key = ver.key
            var isCommonPrefix = false
            var currentPrefix = ""

            if let delimiter = delimiter {
                let prefixLen = prefix?.count ?? 0
                if key.count > prefixLen {
                    let searchRange = key.index(key.startIndex, offsetBy: prefixLen)..<key.endIndex
                    if let range = key.range(of: delimiter, range: searchRange) {
                        currentPrefix = String(key[..<range.upperBound])
                        isCommonPrefix = true
                    }
                }
            }

            if isCommonPrefix {
                if currentPrefix != lastPrefix {
                    commonPrefixes.insert(currentPrefix)
                    lastPrefix = currentPrefix
                    count += 1
                }
            } else {
                versions.append(ver)
                count += 1
            }
        }

        let nextKeyMarker = truncated ? versions.last?.key : nil
        let nextVersionIdMarker = truncated ? versions.last?.versionId : nil

        return ListVersionsResult(
            versions: versions,
            commonPrefixes: Array(commonPrefixes).sorted(),
            isTruncated: truncated,
            nextKeyMarker: nextKeyMarker,
            nextVersionIdMarker: nextVersionIdMarker
        )
    }

    /// Shuts down the metadata store and closes the database connection.
    /// Should be called when the application is terminating to ensure proper cleanup.
    /// - Throws: Database errors if the connection cannot be closed cleanly
    func shutdown() async throws {
        try await connection.close()
    }

    // MARK: - Tagging

    /// Retrieves tags for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket tags
    ///   - versionId: Optional version ID for object tags
    /// - Returns: Array of S3Tag objects
    /// - Throws: Database errors if the query fails
    func getTags(bucket: String, key: String?, versionId: String?) async throws -> [S3Tag] {
        let query: String
        let params: [SQLiteData]

        if let key = key {
            var q = "SELECT tags FROM objects WHERE bucket = ? AND key = ?"
            var p: [SQLiteData] = [.text(bucket), .text(key)]
            if let versionId = versionId {
                q += " AND version_id = ?"
                p.append(.text(versionId))
            } else {
                q += " AND is_latest = 1"
            }
            query = q
            params = p
        } else {
            query = "SELECT tags FROM buckets WHERE name = ?"
            params = [.text(bucket)]
        }

        let rows = try await connection.query(query, params)
        guard let row = rows.first else {
            if key != nil { throw S3Error.noSuchKey } else { throw S3Error.noSuchBucket }
        }

        if let tagsJSON = row.column("tags")?.string,
            let data = tagsJSON.data(using: .utf8),
            let tags = try? JSONDecoder().decode([S3Tag].self, from: data)
        {
            return tags
        }

        return []
    }

    /// Updates tags for a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket tags
    ///   - versionId: Optional version ID for object tags
    ///   - tags: Array of S3Tag objects to set
    /// - Throws: Database errors if the update fails
    func putTags(bucket: String, key: String?, versionId: String?, tags: [S3Tag]) async throws {
        let tagsData = try JSONEncoder().encode(tags)
        let tagsString = String(data: tagsData, encoding: .utf8) ?? "[]"

        if let key = key {
            var query = "UPDATE objects SET tags = ? WHERE bucket = ? AND key = ?"
            var params: [SQLiteData] = [.text(tagsString), .text(bucket), .text(key)]
            if let versionId = versionId {
                query += " AND version_id = ?"
                params.append(.text(versionId))
            } else {
                query += " AND is_latest = 1"
            }
            _ = try await connection.query(query, params)
        } else {
            let query = "UPDATE buckets SET tags = ? WHERE name = ?"
            _ = try await connection.query(query, [.text(tagsString), .text(bucket)])
        }
    }

    /// Removes all tags from a bucket or object.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - key: Optional object key, or nil for bucket tags
    ///   - versionId: Optional version ID for object tags
    /// - Throws: Database errors if the update fails
    func deleteTags(bucket: String, key: String?, versionId: String?) async throws {
        if let key = key {
            var query = "UPDATE objects SET tags = NULL WHERE bucket = ? AND key = ?"
            var params: [SQLiteData] = [.text(bucket), .text(key)]
            if let versionId = versionId {
                query += " AND version_id = ?"
                params.append(.text(versionId))
            } else {
                query += " AND is_latest = 1"
            }
            _ = try await connection.query(query, params)
        } else {
            let query = "UPDATE buckets SET tags = NULL WHERE name = ?"
            _ = try await connection.query(query, [.text(bucket)])
        }
    }
}

extension SQLMetadataStore: UserStore {
    /// Creates a new user account in the system.
    /// - Parameters:
    ///   - username: Display name for the user
    ///   - accessKey: AWS access key ID for authentication
    ///   - secretKey: AWS secret access key for authentication
    /// - Throws: Database errors if user creation fails or access key already exists
    func createUser(username: String, accessKey: String, secretKey: String) async throws {
        let query = "INSERT INTO users (username, access_key, secret_key) VALUES (?, ?, ?)"
        _ = try await connection.query(
            query,
            [
                .text(username),
                .text(accessKey),
                .text(secretKey),
            ])
    }

    /// Retrieves user information by access key.
    /// - Parameter accessKey: The AWS access key ID
    /// - Returns: User object if found, or nil if not found
    /// - Throws: Database errors if the query fails
    func getUser(accessKey: String) async throws -> User? {
        let query = "SELECT username, access_key, secret_key FROM users WHERE access_key = ?"
        let rows = try await connection.query(query, [.text(accessKey)])

        guard let row = rows.first else {
            return nil
        }

        return User(
            username: row.column("username")?.string ?? "",
            accessKey: row.column("access_key")?.string ?? "",
            secretKey: row.column("secret_key")?.string ?? ""
        )
    }

    /// Lists all user accounts in the system.
    /// - Returns: Array of all User objects
    /// - Throws: Database errors if the query fails
    func listUsers() async throws -> [User] {
        let query = "SELECT username, access_key, secret_key FROM users"
        let rows = try await connection.query(query)

        return rows.map { row in
            User(
                username: row.column("username")?.string ?? "",
                accessKey: row.column("access_key")?.string ?? "",
                secretKey: row.column("secret_key")?.string ?? ""
            )
        }
    }

    /// Deletes a user account from the system.
    /// - Parameter accessKey: The AWS access key ID of the user to delete
    /// - Throws: Database errors if the deletion fails
    func deleteUser(accessKey: String) async throws {
        let query = "DELETE FROM users WHERE access_key = ?"
        _ = try await connection.query(query, [.text(accessKey)])
    }

    // MARK: - Lifecycle

    /// Retrieves the lifecycle configuration for a bucket.
    /// - Parameter bucket: The bucket name
    /// - Returns: LifecycleConfiguration if set, or nil if not configured
    /// - Throws: Database errors if the query fails
    func getLifecycle(bucket: String) async throws -> LifecycleConfiguration? {
        let query = "SELECT configuration FROM bucket_lifecycle WHERE bucket_name = ?"
        let rows = try await connection.query(query, [.text(bucket)])
        guard let row = rows.first, let configJSON = row.column("configuration")?.string else {
            return nil
        }
        let data = configJSON.data(using: .utf8) ?? Data()
        return try? JSONDecoder().decode(LifecycleConfiguration.self, from: data)
    }

    /// Sets the lifecycle configuration for a bucket.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - configuration: The lifecycle configuration to apply
    /// - Throws: Database errors if the update fails
    func putLifecycle(bucket: String, configuration: LifecycleConfiguration) async throws {
        let data = try JSONEncoder().encode(configuration)
        let configJSON = String(data: data, encoding: .utf8) ?? ""
        let query =
            "INSERT OR REPLACE INTO bucket_lifecycle (bucket_name, configuration) VALUES (?, ?)"
        _ = try await connection.query(query, [.text(bucket), .text(configJSON)])
    }

    /// Removes the lifecycle configuration from a bucket.
    /// - Parameter bucket: The bucket name
    /// - Throws: Database errors if the deletion fails
    func deleteLifecycle(bucket: String) async throws {
        let query = "DELETE FROM bucket_lifecycle WHERE bucket_name = ?"
        _ = try await connection.query(query, [.text(bucket)])
    }

    // MARK: - Object Lock

    /// Retrieves the object lock configuration for a bucket.
    /// - Parameter bucket: The bucket name
    /// - Returns: The object lock configuration, or nil if not set
    /// - Throws: Database errors if the query fails
    func getObjectLockConfiguration(bucket: String) async throws -> ObjectLockConfiguration? {
        let query = "SELECT configuration FROM bucket_object_lock WHERE bucket_name = ?"
        let rows = try await connection.query(query, [.text(bucket)])

        guard let row = rows.first,
              let configJSON = row.column("configuration")?.string,
              let data = configJSON.data(using: .utf8)
        else {
            return nil
        }

        return try JSONDecoder().decode(ObjectLockConfiguration.self, from: data)
    }

    /// Sets the object lock configuration for a bucket.
    /// - Parameters:
    ///   - bucket: The bucket name
    ///   - configuration: The object lock configuration to apply
    /// - Throws: Database errors if the update fails
    func putObjectLockConfiguration(bucket: String, configuration: ObjectLockConfiguration) async throws {
        let data = try JSONEncoder().encode(configuration)
        let configJSON = String(data: data, encoding: .utf8) ?? ""
        let query =
            "INSERT OR REPLACE INTO bucket_object_lock (bucket_name, configuration) VALUES (?, ?)"
        _ = try await connection.query(query, [.text(bucket), .text(configJSON)])
    }

    // MARK: - Replication

    func getBucketReplication(bucket: String) async throws -> ReplicationConfiguration? {
        let query = "SELECT configuration FROM bucket_replication WHERE bucket_name = ?"
        let result = try await connection.query(query, [.text(bucket)])

        guard let row = result.first,
              let configData = row.column("configuration")?.string?.data(using: .utf8)
        else {
            return nil
        }

        return try JSONDecoder().decode(ReplicationConfiguration.self, from: configData)
    }

    func putBucketReplication(bucket: String, configuration: ReplicationConfiguration) async throws {
        let data = try JSONEncoder().encode(configuration)
        let configJSON = String(data: data, encoding: .utf8) ?? ""
        let query =
            "INSERT OR REPLACE INTO bucket_replication (bucket_name, configuration) VALUES (?, ?)"
        _ = try await connection.query(query, [.text(bucket), .text(configJSON)])
    }

    /// Remove the replication configuration from a bucket.
    /// Disables automatic replication for the bucket.
    ///
    /// - Parameter bucket: Bucket name
    /// - Throws: Error if deletion fails
    func deleteBucketReplication(bucket: String) async throws {
        let query = "DELETE FROM bucket_replication WHERE bucket_name = ?"
        _ = try await connection.query(query, [.text(bucket)])
    }

    // MARK: - Event Notifications

    /// Get the notification configuration for a bucket.
    /// Returns event notification settings for S3 operations.
    ///
    /// - Parameter bucket: Bucket name
    /// - Returns: Notification configuration or nil if not set
    /// - Throws: Error if bucket doesn't exist
    func getBucketNotification(bucket: String) async throws -> NotificationConfiguration? {
        let query = "SELECT configuration FROM bucket_notification WHERE bucket_name = ?"
        let result = try await connection.query(query, [.text(bucket)])

        guard let row = result.first,
              let configData = row.column("configuration")?.string?.data(using: .utf8)
        else {
            return nil
        }

        return try JSONDecoder().decode(NotificationConfiguration.self, from: configData)
    }

    /// Set the notification configuration for a bucket.
    /// Configures event notifications for S3 operations like object creation/deletion.
    ///
    /// - Parameters:
    ///   - bucket: Bucket name
    ///   - configuration: New notification configuration
    /// - Throws: Error if update fails
    func putBucketNotification(bucket: String, configuration: NotificationConfiguration) async throws {
        let data = try JSONEncoder().encode(configuration)
        let configJSON = String(data: data, encoding: .utf8) ?? ""
        let query =
            "INSERT OR REPLACE INTO bucket_notification (bucket_name, configuration) VALUES (?, ?)"
        _ = try await connection.query(query, [.text(bucket), .text(configJSON)])
    }

    /// Remove the notification configuration from a bucket.
    /// Disables event notifications for the bucket.
    ///
    /// - Parameter bucket: Bucket name
    /// - Throws: Error if deletion fails
    func deleteBucketNotification(bucket: String) async throws {
        let query = "DELETE FROM bucket_notification WHERE bucket_name = ?"
        _ = try await connection.query(query, [.text(bucket)])
    }

    // MARK: - VPC Configuration

    func getBucketVpcConfiguration(bucket: String) async throws -> VpcConfiguration? {
        let query = "SELECT vpc_id, allowed_ip_ranges FROM bucket_vpc_config WHERE bucket_name = ?"
        let result = try await connection.query(query, [.text(bucket)])

        guard let row = result.first else {
            return nil
        }

        let vpcId = row.column("vpc_id")?.string
        let ipRangesJSON = row.column("allowed_ip_ranges")?.string ?? "[]"

        let ipRanges = try JSONDecoder().decode([String].self, from: Data(ipRangesJSON.utf8))

        return VpcConfiguration(vpcId: vpcId, allowedIpRanges: ipRanges)
    }

    func putBucketVpcConfiguration(bucket: String, configuration: VpcConfiguration) async throws {
        let ipRangesJSON = String(data: try JSONEncoder().encode(configuration.allowedIpRanges), encoding: .utf8) ?? "[]"
        let query = """
            INSERT OR REPLACE INTO bucket_vpc_config (bucket_name, vpc_id, allowed_ip_ranges)
            VALUES (?, ?, ?)
            """
        _ = try await connection.query(query, [
            .text(bucket),
            .text(configuration.vpcId ?? ""),
            .text(ipRangesJSON)
        ])
    }

    func deleteBucketVpcConfiguration(bucket: String) async throws {
        let query = "DELETE FROM bucket_vpc_config WHERE bucket_name = ?"
        _ = try await connection.query(query, [.text(bucket)])
    }

    // MARK: - Audit Events

    func logAuditEvent(_ event: AuditEvent) async throws {
        let additionalDataJSON = event.additionalData.map { String(data: try! JSONEncoder().encode($0), encoding: .utf8) } ?? "{}"
        let query = """
            INSERT INTO audit_events (
                id, timestamp, event_type, principal, source_ip, user_agent, request_id,
                bucket, key, operation, status, error_message, additional_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        _ = try await connection.query(query, [
            .text(event.id),
            .float(event.timestamp.timeIntervalSince1970),
            .text(event.eventType.rawValue),
            .text(event.principal),
            .text(event.sourceIp ?? ""),
            .text(event.userAgent ?? ""),
            .text(event.requestId),
            .text(event.bucket ?? ""),
            .text(event.key ?? ""),
            .text(event.operation),
            .text(event.status),
            .text(event.errorMessage ?? ""),
            .text(additionalDataJSON ?? "{}")
        ])
    }

    func getAuditEvents(
        bucket: String?, principal: String?, eventType: AuditEventType?, startDate: Date?, endDate: Date?,
        limit: Int?, continuationToken: String?
    ) async throws -> (events: [AuditEvent], nextContinuationToken: String?) {
        var query = """
            SELECT id, timestamp, event_type, principal, source_ip, user_agent, request_id,
                   bucket, key, operation, status, error_message, additional_data
            FROM audit_events WHERE 1=1
            """
        var params: [SQLiteData] = []

        if let bucket = bucket {
            query += " AND bucket = ?"
            params.append(.text(bucket))
        }

        if let principal = principal {
            query += " AND principal = ?"
            params.append(.text(principal))
        }

        if let eventType = eventType {
            query += " AND event_type = ?"
            params.append(.text(eventType.rawValue))
        }

        if let startDate = startDate {
            query += " AND timestamp >= ?"
            params.append(.float(startDate.timeIntervalSince1970))
        }

        if let endDate = endDate {
            query += " AND timestamp <= ?"
            params.append(.float(endDate.timeIntervalSince1970))
        }

        query += " ORDER BY timestamp DESC"

        if let limit = limit {
            query += " LIMIT ?"
            params.append(.integer(limit + 1)) // +1 to check if there are more
        }

        if let continuationToken = continuationToken {
            // Parse continuation token as timestamp
            if let tokenTimestamp = Double(continuationToken) {
                query += " AND timestamp < ?"
                params.append(.float(tokenTimestamp))
            }
        }

        let rows = try await connection.query(query, params)

        var events: [AuditEvent] = []
        var nextToken: String?

        for (index, row) in rows.enumerated() {
            if let limit = limit, index >= limit {
                // This is the extra row, use it for next token
                let timestamp = row.column("timestamp")?.double ?? 0
                nextToken = String(timestamp)
                break
            }

            let id = row.column("id")?.string ?? ""
            let timestamp = Date(timeIntervalSince1970: row.column("timestamp")?.double ?? 0)
            let eventTypeRaw = row.column("event_type")?.string ?? ""
            let eventType = AuditEventType(rawValue: eventTypeRaw) ?? .accessDenied
            let principal = row.column("principal")?.string ?? ""
            let sourceIp = row.column("source_ip")?.string
            let userAgent = row.column("user_agent")?.string
            let requestId = row.column("request_id")?.string ?? ""
            let bucket = row.column("bucket")?.string
            let key = row.column("key")?.string
            let operation = row.column("operation")?.string ?? ""
            let status = row.column("status")?.string ?? ""
            let errorMessage = row.column("error_message")?.string
            let additionalDataJSON = row.column("additional_data")?.string ?? "{}"

            let additionalData = (try? JSONDecoder().decode([String: String].self, from: Data(additionalDataJSON.utf8))) ?? [:]

            let event = AuditEvent(
                id: id,
                timestamp: timestamp,
                eventType: eventType,
                principal: principal,
                sourceIp: sourceIp,
                userAgent: userAgent,
                requestId: requestId,
                bucket: bucket,
                key: key,
                operation: operation,
                status: status,
                errorMessage: errorMessage,
                additionalData: additionalData.isEmpty ? nil : additionalData
            )
            events.append(event)
        }

        return (events: events, nextContinuationToken: nextToken)
    }

    func deleteAuditEvents(olderThan: Date) async throws {
        let query = "DELETE FROM audit_events WHERE timestamp < ?"
        _ = try await connection.query(query, [.float(olderThan.timeIntervalSince1970)])
    }

    // MARK: - Batch Operations

    /// Create a new batch job for large-scale object operations.
    /// Initializes a batch job with the specified configuration and returns the job ID.
    ///
    /// - Parameter job: Batch job configuration
    /// - Returns: Unique job ID for tracking the batch operation
    /// - Throws: Error if job creation fails
    func createBatchJob(job: BatchJob) async throws -> String {
        let operationParamsJSON = String(data: try JSONEncoder().encode(job.operation.parameters), encoding: .utf8) ?? "{}"
        let manifestFieldsJSON = String(data: try JSONEncoder().encode(job.manifest.spec.fields), encoding: .utf8) ?? "[]"
        let failureReasonsJSON = String(data: try JSONEncoder().encode(job.failureReasons), encoding: .utf8) ?? "[]"

        let query = """
            INSERT INTO batch_jobs (
                id, operation_type, operation_parameters,
                manifest_location_bucket, manifest_location_key, manifest_location_etag,
                manifest_spec_format, manifest_spec_fields,
                priority, role_arn, status, created_at, completed_at,
                failure_reasons, total_objects, processed_objects, failed_objects
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        _ = try await connection.query(query, [
            .text(job.id),
            .text(job.operation.type.rawValue),
            .text(operationParamsJSON),
            .text(job.manifest.location.bucket),
            .text(job.manifest.location.key),
            .text(job.manifest.location.etag ?? ""),
            .text(job.manifest.spec.format.rawValue),
            .text(manifestFieldsJSON),
            .integer(Int(job.priority)),
            .text(job.roleArn ?? ""),
            .text(job.status.rawValue),
            .float(job.createdAt.timeIntervalSince1970),
            .float(job.completedAt?.timeIntervalSince1970 ?? 0),
            .text(failureReasonsJSON),
            .integer(job.progress.totalObjects),
            .integer(job.progress.processedObjects),
            .integer(job.progress.failedObjects)
        ])

        return job.id
    }

    /// Retrieve a batch job by its ID.
    /// Returns the current status and progress of the batch operation.
    ///
    /// - Parameter jobId: Unique batch job identifier
    /// - Returns: Batch job information or nil if not found
    /// - Throws: Error if query fails
    func getBatchJob(jobId: String) async throws -> BatchJob? {
        let query = """
            SELECT id, operation_type, operation_parameters,
                   manifest_location_bucket, manifest_location_key, manifest_location_etag,
                   manifest_spec_format, manifest_spec_fields,
                   priority, role_arn, status, created_at, completed_at,
                   failure_reasons, total_objects, processed_objects, failed_objects
            FROM batch_jobs WHERE id = ?
            """

        let rows = try await connection.query(query, [.text(jobId)])
        guard let row = rows.first else { return nil }

        let operationTypeRaw = row.column("operation_type")?.string ?? ""
        let operationType = BatchOperationType(rawValue: operationTypeRaw) ?? .s3DeleteObject
        let operationParamsJSON = row.column("operation_parameters")?.string ?? "{}"
        let operationParams = try JSONDecoder().decode([String: String].self, from: Data(operationParamsJSON.utf8))

        let manifestBucket = row.column("manifest_location_bucket")?.string ?? ""
        let manifestKey = row.column("manifest_location_key")?.string ?? ""
        let manifestEtag = row.column("manifest_location_etag")?.string
        let manifestFormatRaw = row.column("manifest_spec_format")?.string ?? ""
        let manifestFormat = BatchManifestFormat(rawValue: manifestFormatRaw) ?? .s3BatchOperationsCsv20180820
        let manifestFieldsJSON = row.column("manifest_spec_fields")?.string ?? "[]"
        let manifestFields = try JSONDecoder().decode([String].self, from: Data(manifestFieldsJSON.utf8))

        let priority = row.column("priority")?.integer ?? 0
        let roleArn = row.column("role_arn")?.string
        let statusRaw = row.column("status")?.string ?? ""
        let status = BatchJobStatus(rawValue: statusRaw) ?? .pending
        let createdAt = Date(timeIntervalSince1970: row.column("created_at")?.double ?? 0)
        let completedAt = row.column("completed_at")?.double.flatMap { $0 > 0 ? Date(timeIntervalSince1970: $0) : nil }
        let failureReasonsJSON = row.column("failure_reasons")?.string ?? "[]"
        let failureReasons = try JSONDecoder().decode([String].self, from: Data(failureReasonsJSON.utf8))
        let totalObjects = row.column("total_objects")?.integer ?? 0
        let processedObjects = row.column("processed_objects")?.integer ?? 0
        let failedObjects = row.column("failed_objects")?.integer ?? 0

        let operation = BatchOperation(type: operationType, parameters: operationParams)
        let manifestLocation = BatchManifestLocation(bucket: manifestBucket, key: manifestKey, etag: manifestEtag)
        let manifestSpec = BatchManifestSpec(format: manifestFormat, fields: manifestFields)
        let manifest = BatchManifest(location: manifestLocation, spec: manifestSpec)
        let progress = BatchProgress(totalObjects: totalObjects, processedObjects: processedObjects, failedObjects: failedObjects)

        return BatchJob(
            id: jobId,
            operation: operation,
            manifest: manifest,
            priority: Int(priority),
            roleArn: roleArn,
            status: status,
            createdAt: createdAt,
            completedAt: completedAt,
            failureReasons: failureReasons,
            progress: progress
        )
    }

    func listBatchJobs(bucket: String?, status: BatchJobStatus?, limit: Int?, continuationToken: String?) async throws -> (jobs: [BatchJob], nextContinuationToken: String?) {
        var query = """
            SELECT id, operation_type, operation_parameters,
                   manifest_location_bucket, manifest_location_key, manifest_location_etag,
                   manifest_spec_format, manifest_spec_fields,
                   priority, role_arn, status, created_at, completed_at,
                   failure_reasons, total_objects, processed_objects, failed_objects
            FROM batch_jobs WHERE 1=1
            """
        var params: [SQLiteData] = []

        if let bucket = bucket {
            query += " AND manifest_location_bucket = ?"
            params.append(.text(bucket))
        }

        if let status = status {
            query += " AND status = ?"
            params.append(.text(status.rawValue))
        }

        query += " ORDER BY created_at DESC"

        if let limit = limit {
            query += " LIMIT ?"
            params.append(.integer(limit))
        }

        let rows = try await connection.query(query, params)
        var jobs: [BatchJob] = []

        for row in rows {
            let jobId = row.column("id")?.string ?? ""
            if let job = try await getBatchJob(jobId: jobId) {
                jobs.append(job)
            }
        }

        // For now, no continuation token support - return all results
        return (jobs: jobs, nextContinuationToken: nil)
    }

    /// Update the status of a batch job.
    /// Changes the job status and optionally records failure messages.
    ///
    /// - Parameters:
    ///   - jobId: Unique batch job identifier
    ///   - status: New job status
    ///   - message: Optional failure message
    /// - Throws: Error if update fails
    func updateBatchJobStatus(jobId: String, status: BatchJobStatus, message: String?) async throws {
        let failureReasonsJSON = String(data: try JSONEncoder().encode(message != nil ? [message!] : []), encoding: .utf8) ?? "[]"
        let completedAt = status == .complete || status == .failed || status == .cancelled ?
            Date().timeIntervalSince1970 : 0

        let query = """
            UPDATE batch_jobs SET status = ?, failure_reasons = ?, completed_at = ? WHERE id = ?
            """
        _ = try await connection.query(query, [
            .text(status.rawValue),
            .text(failureReasonsJSON),
            .float(completedAt),
            .text(jobId)
        ])
    }

    func deleteBatchJob(jobId: String) async throws {
        let query = "DELETE FROM batch_jobs WHERE id = ?"
        _ = try await connection.query(query, [.text(jobId)])
    }

    func executeBatchOperation(jobId: String, bucket: String, key: String) async throws {
        // Get the job details
        guard let job = try await getBatchJob(jobId: jobId) else {
            throw S3Error.noSuchKey // Or a more specific error
        }

        // Update progress counters
        let updateQuery = """
            UPDATE batch_jobs SET processed_objects = processed_objects + 1 WHERE id = ?
            """
        _ = try await connection.query(updateQuery, [.text(jobId)])

        // For now, we'll implement basic operations. In a full implementation,
        // this would execute the specific operation type on the object.
        // For this demo, we'll just log the operation.
        logger.info("Executing batch operation", metadata: [
            "jobId": .string(jobId),
            "operation": .string(job.operation.type.rawValue),
            "bucket": .string(bucket),
            "key": .string(key)
        ])
    }
}
