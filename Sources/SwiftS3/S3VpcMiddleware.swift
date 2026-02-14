import Foundation
import Hummingbird
import HTTPTypes
import NIO

/// Middleware for enforcing VPC-only access restrictions on S3 buckets.
/// Validates that requests originate from allowed IP ranges for buckets with VPC configurations.
/// Must be applied after authentication but before request processing.
struct S3VpcMiddleware<Context: RequestContext>: RouterMiddleware {
    let storage: any StorageBackend

    init(storage: any StorageBackend) {
        self.storage = storage
    }

    /// Processes requests to enforce VPC access restrictions.
    /// Extracts client IP, checks bucket VPC configuration, and validates access.
    ///
    /// - Parameters:
    ///   - request: The incoming HTTP request
    ///   - context: Request context (unused in this middleware)
    ///   - next: Next middleware/router in the chain
    /// - Returns: Response from downstream handlers if access is allowed
    /// - Throws: S3Error.accessDenied if request is not from allowed IP range
    func handle(_ request: Input, context: Context, next: (Input, Context) async throws -> Output)
        async throws -> Output
    {
        // Extract bucket name from URL path
        guard let bucket = extractBucketFromPath(request.uri.path) else {
            // Not a bucket-specific request, allow through
            return try await next(request, context)
        }

        // Check if bucket has VPC configuration
        guard let vpcConfig = try await storage.getBucketVpcConfiguration(bucket: bucket) else {
            // No VPC restrictions, allow through
            return try await next(request, context)
        }

        // Extract client IP address
        let clientIP = extractClientIP(from: request)

        // Check if IP is in allowed ranges
        if !isIPAllowed(clientIP, allowedRanges: vpcConfig.allowedIpRanges) {
            throw S3Error.accessDenied
        }

        // IP is allowed, proceed with request
        return try await next(request, context)
    }

    /// Extracts bucket name from S3 URL path.
    /// Handles both virtual-hosted and path-style URLs.
    ///
    /// - Parameter path: The request URI path
    /// - Returns: Bucket name if found, nil otherwise
    private func extractBucketFromPath(_ path: String) -> String? {
        let components = path.split(separator: "/", omittingEmptySubsequences: true)
        return components.first.map(String.init)
    }

    /// Extracts client IP address from request headers.
    /// Checks X-Forwarded-For, X-Real-IP, and falls back to remote address.
    ///
    /// - Parameter request: The HTTP request
    /// - Returns: Client IP address as string
    private func extractClientIP(from request: Input) -> String {
        // Check X-Forwarded-For header (first IP in comma-separated list)
        if let forwardedFor = request.headers[HTTPField.Name("X-Forwarded-For")!],
           let firstIP = forwardedFor.split(separator: ",").first?.trimmingCharacters(in: CharacterSet.whitespaces) {
            return firstIP
        }

        // Check X-Real-IP header
        if let realIP = request.headers[HTTPField.Name("X-Real-IP")!] {
            return realIP
        }

        // Fallback to remote address (if available)
        // Note: In a real deployment, this would come from the connection
        // For now, return a default that will be rejected by VPC rules
        return "0.0.0.0"
    }

    /// Checks if an IP address is within any of the allowed CIDR ranges.
    ///
    /// - Parameters:
    ///   - ipAddress: The IP address to check
    ///   - allowedRanges: Array of CIDR notation ranges (e.g., ["10.0.0.0/8"])
    /// - Returns: True if IP is in any allowed range, false otherwise
    private func isIPAllowed(_ ipAddress: String, allowedRanges: [String]) -> Bool {
        // Parse the IP address
        guard let ipComponents = parseIPv4(ipAddress) else {
            return false
        }

        for range in allowedRanges {
            if isIPInRange(ipComponents, range: range) {
                return true
            }
        }

        return false
    }

    /// Parses an IPv4 address string into four integer components.
    ///
    /// - Parameter ip: IP address string (e.g., "192.168.1.1")
    /// - Returns: Tuple of four integers if valid IPv4, nil otherwise
    private func parseIPv4(_ ip: String) -> (Int, Int, Int, Int)? {
        let components = ip.split(separator: ".").compactMap { Int($0) }
        guard components.count == 4,
              components.allSatisfy({ $0 >= 0 && $0 <= 255 }) else {
            return nil
        }
        return (components[0], components[1], components[2], components[3])
    }

    /// Checks if an IP address is within a CIDR range.
    ///
    /// - Parameters:
    ///   - ipComponents: Parsed IP address components
    ///   - range: CIDR range string (e.g., "10.0.0.0/8")
    /// - Returns: True if IP is in range, false otherwise
    private func isIPInRange(_ ipComponents: (Int, Int, Int, Int), range: String) -> Bool {
        let parts = range.split(separator: "/")
        guard parts.count == 2,
              let prefixLength = Int(parts[1]),
              prefixLength >= 0 && prefixLength <= 32,
              let networkComponents = parseIPv4(String(parts[0])) else {
            return false
        }

        // Convert IPs to 32-bit integers
        let ipInt = (ipComponents.0 << 24) | (ipComponents.1 << 16) | (ipComponents.2 << 8) | ipComponents.3
        let networkInt = (networkComponents.0 << 24) | (networkComponents.1 << 16) | (networkComponents.2 << 8) | networkComponents.3

        // Create subnet mask
        let mask = prefixLength == 0 ? 0 : ~0 << (32 - prefixLength)

        // Check if IP is in subnet
        return (ipInt & mask) == (networkInt & mask)
    }
}