import Crypto
import Foundation
import HTTPTypes
import Hummingbird
import HummingbirdTesting

@testable import SwiftS3

/// Helper to generate AWS Signature V4 headers for testing
struct AWSAuthHelper {
    let accessKey: String
    let secretKey: String
    let region: String
    let service: String

    init(
        accessKey: String = "admin", secretKey: String = "password", region: String = "us-east-1",
        service: String = "s3"
    ) {
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.region = region
        self.service = service
    }

    func signRequest(
        method: HTTPRequest.Method,
        url: URL,  // URL must contain scheme, host, port (if any), path, and query
        payload: String = "",  // Empty string for empty body
        date: Date = Date()
    ) throws -> HTTPFields {
        let methodStr = method.rawValue
        let path = url.path
        let query = url.query ?? ""
        let host = url.host ?? "localhost"
        let port = url.port

        let isoDateFormatter = ISO8601DateFormatter()
        isoDateFormatter.formatOptions = [
            .withYear, .withMonth, .withDay, .withTime, .withTimeZone,
        ]
        // AWS requires basic format: YYYYMMDDTHHMMSSZ, ISO8601 usually has separators
        // Custom formatter for AWS
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        dateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        let amzDate = dateFormatter.string(from: date)

        dateFormatter.dateFormat = "yyyyMMdd"
        let dateStamp = dateFormatter.string(from: date)

        // 1. Canonical Headers
        var canonicalHeaders = ""
        var signedHeaders = ""

        // Host header
        var hostHeader = host
        if let port = port {
            hostHeader += ":\(port)"
        }

        canonicalHeaders += "host:\(hostHeader)\n"
        canonicalHeaders += "x-amz-date:\(amzDate)\n"
        signedHeaders = "host;x-amz-date"

        // Payload Hash
        let payloadData = Data(payload.utf8)
        let payloadHash = SHA256.hash(data: payloadData).map { String(format: "%02x", $0) }.joined()

        // Canonical Request
        // Query must be sorted. URL.query doesn't guarantee this but for simple tests we manage it.
        // Let's assume input query is already encoded or simple.
        // For strictness we should parse and sort.
        var canonicalQuery = ""
        if !query.isEmpty {
            let items = query.split(separator: "&").sorted()
            canonicalQuery = items.joined(separator: "&")
        }

        let canonicalRequest = """
            \(methodStr)
            \(path)
            \(canonicalQuery)
            \(canonicalHeaders)
            \(signedHeaders)
            \(payloadHash)
            """

        // 2. String to Sign
        let algorithm = "AWS4-HMAC-SHA256"
        let credentialScope = "\(dateStamp)/\(region)/\(service)/aws4_request"
        let canonicalRequestHash = SHA256.hash(data: Data(canonicalRequest.utf8)).map {
            String(format: "%02x", $0)
        }.joined()

        let stringToSign = """
            \(algorithm)
            \(amzDate)
            \(credentialScope)
            \(canonicalRequestHash)
            """

        // 3. Signature
        let kSecret = "AWS4" + secretKey
        let kDate = try hmac(key: Data(kSecret.utf8), data: Data(dateStamp.utf8))
        let kRegion = try hmac(key: kDate, data: Data(region.utf8))
        let kService = try hmac(key: kRegion, data: Data(service.utf8))
        let kSigning = try hmac(key: kService, data: Data("aws4_request".utf8))

        let signatureData = try hmac(key: kSigning, data: Data(stringToSign.utf8))
        let signature = signatureData.map { String(format: "%02x", $0) }.joined()

        let authorization =
            "\(algorithm) Credential=\(accessKey)/\(credentialScope), SignedHeaders=\(signedHeaders), Signature=\(signature)"

        var fields = HTTPFields()
        fields[.authorization] = authorization
        fields[.init("x-amz-date")!] = amzDate
        fields[.init("x-amz-content-sha256")!] = payloadHash
        fields[.init("Host")!] = hostHeader  // Hummingbird/NIO might set this auto but we need it for signing match

        return fields
    }

    private func hmac(key: Data, data: Data) throws -> Data {
        let auth = HMAC<SHA256>.authenticationCode(for: data, using: SymmetricKey(data: key))
        return Data(auth)
    }
}
