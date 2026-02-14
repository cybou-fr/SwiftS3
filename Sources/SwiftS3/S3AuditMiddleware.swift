import Foundation
import HTTPTypes
import Hummingbird
import Logging

/// Middleware for logging audit events for compliance and security monitoring.
/// Captures all S3 operations with detailed information about principals, operations,
/// and outcomes for security auditing and compliance reporting.
struct S3AuditMiddleware<Context: RequestContext>: RouterMiddleware {
    let storage: any StorageBackend
    let logger: Logger

    /// Initializes the audit middleware with storage backend and logger
    /// - Parameters:
    ///   - storage: Storage backend for retrieving audit data
    ///   - logger: Logger for audit events (defaults to SwiftS3.Audit label)
    init(storage: any StorageBackend, logger: Logger = Logger(label: "SwiftS3.Audit")) {
        self.storage = storage
        self.logger = logger
    }

    /// Handles incoming requests by logging audit events for compliance and security monitoring
    /// - Parameters:
    ///   - request: The incoming HTTP request
    ///   - context: Request context containing request metadata
    ///   - next: Next middleware in the chain
    /// - Returns: The response from the next middleware
    /// - Throws: Any error thrown by the next middleware
    func handle(_ request: Request, context: Context, next: (Request, Context) async throws -> Response) async throws -> Response {
        let startTime = Date()
        let requestId = UUID().uuidString

        // Extract principal information
        let principal = extractPrincipal(from: request)

        // Extract source IP
        let sourceIp = extractSourceIp(from: request)

        // Extract user agent
        let userAgent = request.headers[.userAgent]

        // Extract bucket and key from path
        let (bucket, key) = extractBucketAndKey(from: request.uri.path)

        // Determine operation
        let operation = extractOperation(from: request)

        var response: Response?
        var error: Error?

        do {
            response = try await next(request, context)
        } catch let e {
            error = e
            throw e
        }

        // Determine event type and status
        let (eventType, status) = determineEventTypeAndStatus(from: request, response: response, error: error)

        // Create audit event
        let auditEvent = AuditEvent(
            timestamp: startTime,
            eventType: eventType,
            principal: principal,
            sourceIp: sourceIp,
            userAgent: userAgent,
            requestId: requestId,
            bucket: bucket,
            key: key,
            operation: operation,
            status: status,
            errorMessage: error?.localizedDescription,
            additionalData: extractAdditionalData(from: request, response: response)
        )

        // Log the event synchronously (for now, to avoid potential crashes)
        do {
            try await storage.logAuditEvent(auditEvent)
            logger.debug("Audit event logged: \(auditEvent.eventType.rawValue) - \(auditEvent.operation)")
        } catch {
            logger.error("Failed to log audit event: \(error)")
        }

        return response!
    }

    /// Extracts the principal (access key) from the request for audit logging
    /// - Parameter request: The HTTP request containing authentication information
    /// - Returns: The access key ID of the authenticated principal, or "anonymous" if not found
    private func extractPrincipal(from request: Request) -> String {
        // Try to extract from Authorization header
        if let authHeader = request.headers[.authorization] {
            // Parse AWS signature format: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
            if authHeader.contains("Credential=") {
                let components = authHeader.split(separator: "Credential=")
                if components.count > 1 {
                    let credential = components[1].split(separator: "/").first ?? ""
                    return String(credential)
                }
            }
        }

        // Fallback to anonymous
        return "anonymous"
    }

    /// Extracts the source IP address from the request headers
    /// - Parameter request: The HTTP request containing IP information
    /// - Returns: The source IP address, or nil if not found
    private func extractSourceIp(from request: Request) -> String? {
        // Try X-Forwarded-For header first (for proxies/load balancers)
        if let forwardedFor = request.headers[HTTPField.Name("X-Forwarded-For")!] {
            return forwardedFor.split(separator: ",").first?.trimmingCharacters(in: CharacterSet.whitespaces)
        }

        // Try X-Real-IP header
        if let realIp = request.headers[HTTPField.Name("X-Real-IP")!] {
            return realIp
        }

        // For local development, return localhost
        return "127.0.0.1"
    }

    /// Extracts bucket and key information from the request path
    /// - Parameter path: The URL path of the request
    /// - Returns: A tuple containing the bucket name and object key, or nil if not parseable
    private func extractBucketAndKey(from path: String) -> (bucket: String?, key: String?) {
        let components = path.split(separator: "/").filter { !$0.isEmpty }

        guard components.count >= 1 else { return (nil, nil) }

        let bucket = String(components[0])

        if components.count >= 2 {
            let key = components[1...].joined(separator: "/")
            return (bucket, key)
        }

        return (bucket, nil)
    }

    /// Extracts the operation type from the HTTP request method and headers
    /// - Parameter request: The HTTP request containing method and headers
    /// - Returns: A string describing the S3 operation being performed
    private func extractOperation(from request: Request) -> String {
        let method = request.method.rawValue
        let path = request.uri.path

        // Extract operation from query parameters or headers
        if let queryOp = request.uri.queryParameters.get("action") {
            return "\(method) \(queryOp)"
        }

        // Common S3 operations based on method and path patterns
        if path.contains("/?") {
            let query = String(path.split(separator: "?").last ?? "")
            return "\(method) \(query)"
        }

        return "\(method) \(path)"
    }

    /// Determines the audit event type and status based on request/response/error information
    /// - Parameters:
    ///   - request: The HTTP request
    ///   - response: The HTTP response, if any
    ///   - error: Any error that occurred during processing
    /// - Returns: A tuple containing the audit event type and status string
    private func determineEventTypeAndStatus(from request: Request, response: Response?, error: Error?) -> (AuditEventType, String) {
        let method = request.method
        let path = request.uri.path

        var eventType = AuditEventType.accessDenied
        var status = "200"

        if let error = error {
            status = "500" // Generic error
            if let s3Error = error as? S3Error {
                switch s3Error {
                case .accessDenied, .invalidAccessKeyId, .signatureDoesNotMatch:
                    eventType = .authenticationFailed
                default:
                    eventType = .accessDenied
                }
            }
            return (eventType, status)
        }

        if let response = response {
            status = String(response.status.code)
        }

        // Determine event type based on method and path
        if method == .put {
            if path.hasSuffix("?lifecycle") {
                eventType = .lifecycleUpdated
            } else if path.hasSuffix("?versioning") {
                eventType = .versioningUpdated
            } else if path.hasSuffix("?replication") {
                eventType = .replicationUpdated
            } else if path.hasSuffix("?notification") {
                eventType = .notificationUpdated
            } else if path.hasSuffix("?acl") {
                eventType = .aclUpdated
            } else if path.hasSuffix("?policy") {
                eventType = .policyUpdated
            } else if path.hasSuffix("?vpc") {
                eventType = .vpcConfigUpdated
            } else if path.contains("/?") == false && !path.contains("?partNumber") && !path.contains("?uploadId") {
                eventType = .objectUploaded
            }
        } else if method == .get {
            if path.contains("/?") == false {
                eventType = .objectDownloaded
            }
        } else if method == .delete {
            if path.hasSuffix("?lifecycle") {
                eventType = .lifecycleUpdated
            } else if path.hasSuffix("?replication") {
                eventType = .replicationUpdated
            } else if path.hasSuffix("?notification") {
                eventType = .notificationUpdated
            } else if path.hasSuffix("?vpc") {
                eventType = .vpcConfigUpdated
            } else {
                eventType = .objectDeleted
            }
        } else if method == .post {
            if path.contains("?delete") {
                eventType = .objectDeleted
            } else if path.contains("?uploads") {
                eventType = .objectUploaded
            }
        }

        return (eventType, status)
    }

    /// Extracts additional metadata from request and response for audit logging
    /// - Parameters:
    ///   - request: The HTTP request
    ///   - response: The HTTP response, if any
    /// - Returns: Dictionary of additional audit data, or nil if no data available
    private func extractAdditionalData(from request: Request, response: Response?) -> [String: String]? {
        var data: [String: String] = [:]

        // Add content length if available
        if let contentLength = request.headers[.contentLength] {
            data["contentLength"] = contentLength
        }

        // Add content type if available
        if let contentType = request.headers[.contentType] {
            data["contentType"] = contentType
        }

        // Add response content length if available
        if let response = response, let contentLength = response.headers[.contentLength] {
            data["responseContentLength"] = contentLength
        }

        return data.isEmpty ? nil : data
    }
}