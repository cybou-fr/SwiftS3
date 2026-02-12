import Foundation
import Logging
import NIO

/// Background task that periodically checks for and deletes expired objects based on Lifecycle Rules
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

    func stop() {
        task?.cancel()
        task = nil
    }

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
        logger.info("Janitor completed lifecycle expiration check")
    }

    private func applyRule(_ rule: LifecycleConfiguration.Rule, to bucket: String) async throws {
        // For now, only simple Expiration Days is implemented
        guard let expiration = rule.expiration, let days = expiration.days else {
            return
        }

        let prefix = rule.filter.prefix
        let cutoffDate = Date().addingTimeInterval(-TimeInterval(Double(days) * 24 * 3600))

        var marker: String? = nil
        var isTruncated = true

        while isTruncated {
            let result = try await storage.listObjects(
                bucket: bucket, prefix: prefix, delimiter: nil, marker: marker,
                continuationToken: nil, maxKeys: pageSize)

            for object in result.objects {
                if object.lastModified < cutoffDate {
                    logger.info(
                        "Janitor expiring object",
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
            marker = result.nextMarker ?? result.objects.last?.key
        }
    }
}
