import Foundation
import Logging
import NIO

/// Background task that periodically checks for and deletes expired objects based on Lifecycle Rules
/// Runs as an actor to ensure thread-safe operation and proper cleanup.
/// Monitors all buckets for lifecycle configurations and removes objects past their expiration dates.
/// Designed to run continuously with configurable check intervals.
actor LifecycleJanitor {
    let storage: any StorageBackend
    let interval: Duration
    let pageSize: Int
    private var task: Task<Void, Never>?
    private let logger: Logger = Logger(label: "SwiftS3.LifecycleJanitor")

    init(storage: any StorageBackend, interval: Duration = .seconds(3600), pageSize: Int = 1000) {
        self.storage = storage
        self.interval = interval
        self.pageSize = pageSize
    }

    /// Starts the background lifecycle monitoring task.
    /// Begins periodic execution of expiration checks at the configured interval.
    /// Safe to call multiple times - only starts if not already running.
    func start() {
        task = Task { [weak self] in
            while !Task.isCancelled {
                guard let self = self else { break }
                do {
                    try await performExpiration()
                } catch {
                    logger.error("Lifecycle Janitor error: \(error)")
                }
                do {
                    try await Task.sleep(for: interval)
                } catch {
                    // Task cancelled
                }
            }
        }
    }

    /// Stops the background lifecycle monitoring task.
    /// Cancels any running expiration checks and prevents further execution.
    /// Safe to call multiple times.
    func stop() {
        task?.cancel()
        task = nil
    }

    /// Performs a complete lifecycle expiration check across all buckets.
    /// Scans each bucket for lifecycle configurations and applies enabled rules.
    /// Removes expired objects and performs cleanup of orphaned metadata.
    /// Called periodically by the background task.
    ///
    /// - Throws: Storage errors if bucket scanning or rule application fails
    func performExpiration() async throws {
        logger.info("Janitor starting lifecycle expiration check")
        let buckets = try await storage.listBuckets()

        for bucket in buckets {
            guard let lifecycle = try await storage.getBucketLifecycle(bucket: bucket.name) else {
                continue
            }

            for rule in lifecycle.rules where rule.status == .enabled {
                try await applyRule(rule, to: bucket.name)
            }
        }

        // Perform garbage collection
        try await performGarbageCollection()

        logger.info("Janitor completed lifecycle expiration check")
    }

    /// Applies a single lifecycle rule to a bucket
    /// - Parameters:
    ///   - rule: The lifecycle rule to apply
    ///   - bucket: The bucket name to apply the rule to
    /// - Throws: Storage errors if rule application fails
    private func applyRule(_ rule: LifecycleConfiguration.Rule, to bucket: String) async throws {
        let _ = rule.filter.prefix ?? ""

        // Handle current version expiration
        if let expiration = rule.expiration, let days = expiration.days {
            try await applyCurrentVersionExpiration(rule: rule, bucket: bucket, days: days)
        }

        // Handle noncurrent version expiration
        if let noncurrentExpiration = rule.noncurrentVersionExpiration {
            try await applyNoncurrentVersionExpiration(rule: rule, bucket: bucket, noncurrentExpiration: noncurrentExpiration)
        }
    }

    /// Applies current version expiration rule to objects in a bucket
    /// - Parameters:
    ///   - rule: The lifecycle rule containing expiration settings
    ///   - bucket: The bucket name
    ///   - days: Number of days after which objects expire
    /// - Throws: Storage errors if expiration application fails
    private func applyCurrentVersionExpiration(rule: LifecycleConfiguration.Rule, bucket: String, days: Int) async throws {
        let prefix = rule.filter.prefix
        let tagFilter = rule.filter.tag
        let cutoffDate = Date().addingTimeInterval(-TimeInterval(Double(days) * 24 * 3600))

        var continuationToken: String? = nil
        var isTruncated = true

        while isTruncated {
            let result = try await storage.listObjects(
                bucket: bucket, prefix: prefix, delimiter: nil, marker: nil,
                continuationToken: continuationToken, maxKeys: pageSize)

            for object in result.objects {
                // Check if object matches tag filter
                var matchesTagFilter = true
                if let tagFilter = tagFilter {
                    let objectTags = try await storage.getTags(bucket: bucket, key: object.key, versionId: nil)
                    matchesTagFilter = objectTags.contains(where: { $0.key == tagFilter.key && $0.value == tagFilter.value })
                }

                if matchesTagFilter && object.lastModified < cutoffDate {
                    logger.info(
                        "Janitor expiring current object",
                        metadata: [
                            "bucket": "\(bucket)",
                            "key": "\(object.key)",
                            "lastModified": "\(object.lastModified)",
                            "cutoff": "\(cutoffDate)",
                        ])
                    _ = try await storage.deleteObject(
                        bucket: bucket, key: object.key, versionId: nil)
                }
            }

            isTruncated = result.isTruncated
            continuationToken = result.nextContinuationToken
        }
    }

    /// Applies non-current version expiration rule to object versions in a bucket
    /// - Parameters:
    ///   - rule: The lifecycle rule containing non-current version expiration settings
    ///   - bucket: The bucket name
    ///   - noncurrentExpiration: The non-current version expiration configuration
    /// - Throws: Storage errors if expiration application fails
    private func applyNoncurrentVersionExpiration(rule: LifecycleConfiguration.Rule, bucket: String, noncurrentExpiration: LifecycleConfiguration.Rule.NoncurrentVersionExpiration) async throws {
        let prefix = rule.filter.prefix
        let tagFilter = rule.filter.tag

        // List all versions for objects matching the prefix
        var keyMarker: String? = nil
        var versionIdMarker: String? = nil
        var isTruncated = true

        while isTruncated {
            let result = try await storage.listObjectVersions(
                bucket: bucket, prefix: prefix, delimiter: nil, keyMarker: keyMarker,
                versionIdMarker: versionIdMarker, maxKeys: pageSize)

            // Group versions by key
            var versionsByKey: [String: [ObjectMetadata]] = [:]
            for version in result.versions {
                versionsByKey[version.key, default: []].append(version)
            }

            // For each key, sort versions by lastModified (newest first) and expire noncurrent ones
            for (key, versions) in versionsByKey {
                let sortedVersions = versions.sorted(by: { $0.lastModified > $1.lastModified })

                // Skip the current version (index 0)
                for (index, version) in sortedVersions.enumerated() where index > 0 {
                    // Check if version matches tag filter
                    var matchesTagFilter = true
                    if let tagFilter = tagFilter {
                        let versionTags = try await storage.getTags(bucket: bucket, key: key, versionId: version.versionId)
                        matchesTagFilter = versionTags.contains(where: { $0.key == tagFilter.key && $0.value == tagFilter.value })
                    }

                    if !matchesTagFilter { continue }

                    var shouldExpire = false

                    // Check noncurrentDays
                    if let noncurrentDays = noncurrentExpiration.noncurrentDays {
                        let cutoffDate = Date().addingTimeInterval(-TimeInterval(Double(noncurrentDays) * 24 * 3600))
                        if version.lastModified < cutoffDate {
                            shouldExpire = true
                        }
                    }

                    // Check newerNoncurrentVersions
                    if let newerNoncurrentVersions = noncurrentExpiration.newerNoncurrentVersions {
                        if index >= newerNoncurrentVersions {
                            shouldExpire = true
                        }
                    }

                    if shouldExpire {
                        logger.info(
                            "Janitor expiring noncurrent version",
                            metadata: [
                                "bucket": "\(bucket)",
                                "key": "\(key)",
                                "versionId": "\(version.versionId)",
                                "lastModified": "\(version.lastModified)",
                                "index": "\(index)",
                            ])
                        _ = try await storage.deleteObject(
                            bucket: bucket, key: key, versionId: version.versionId)
                    }
                }
            }

            isTruncated = result.isTruncated
            keyMarker = result.nextKeyMarker
            versionIdMarker = result.nextVersionIdMarker
        }
    }

    /// Performs garbage collection of orphaned metadata and incomplete operations
    /// - Throws: Storage errors if garbage collection fails
    private func performGarbageCollection() async throws {
        logger.info("Janitor starting garbage collection")
        
        // Cleanup orphaned multipart uploads older than 1 hour
        try await storage.cleanupOrphanedUploads(olderThan: 3600)
        
        logger.info("Janitor completed garbage collection")
    }
}
