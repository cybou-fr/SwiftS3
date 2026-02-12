import Foundation
import HTTPTypes
import Hummingbird
import Logging

/// Middleware for logging S3 API requests and responses.
/// Records request method, path, response status, and timing information.
struct S3RequestLogger<Context: RequestContext>: RouterMiddleware {
    let logger = Logger(label: "SwiftS3.Request")

    /// Logs incoming requests and their responses with timing information.
    func handle(
        _ request: Input, context: Context,
        next: (Input, Context) async throws -> Output
    ) async throws -> Output {
        let start = ContinuousClock.now
        do {
            let response = try await next(request, context)
            let duration = ContinuousClock.now - start
            logger.info(
                "\(request.method) \(request.uri.path) → \(response.status.code)",
                metadata: [
                    "method": "\(request.method.rawValue)",
                    "path": "\(request.uri.path)",
                    "status": "\(response.status.code)",
                    "duration_ms": "\(duration.components.attoseconds / 1_000_000_000_000_000)",
                ])
            return response
        } catch {
            let duration = ContinuousClock.now - start
            logger.error(
                "\(request.method) \(request.uri.path) → ERROR",
                metadata: [
                    "method": "\(request.method.rawValue)",
                    "path": "\(request.uri.path)",
                    "error": "\(error)",
                    "duration_ms": "\(duration.components.attoseconds / 1_000_000_000_000_000)",
                ])
            throw error
        }
    }
}
