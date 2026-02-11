import Crypto
import Foundation
import HTTPTypes
import Hummingbird

struct S3Authenticator<Context: RequestContext>: RouterMiddleware {
    let accessKey: String
    let secretKey: String

    init(accessKey: String = "admin", secretKey: String = "password") {
        self.accessKey = accessKey
        self.secretKey = secretKey
    }

    func handle(_ request: Input, context: Context, next: (Input, Context) async throws -> Output)
        async throws -> Output
    {
        // 1. Check if Authorization header exists. If not, it might be an anonymous request.
        guard let authHeader = request.headers[.authorization] else {
            // If no auth header, proceed (anonymous access) or fail?
            return try await next(request, context)
        }

        // 2. Parse Auth Header
        guard authHeader.starts(with: "AWS4-HMAC-SHA256") else {
            return try await next(request, context)
        }

        // 3. Verification Logic
        let isValid = try await verifySignature(request: request, authHeader: authHeader)

        guard isValid else {
            throw S3Error.signatureDoesNotMatch
        }

        return try await next(request, context)
    }

    // ... Placeholder for helper methods
    func verifySignature(request: Request, authHeader: String) async throws -> Bool {
        // Parse Authorization Header
        // Example: AWS4-HMAC-SHA256 Credential=admin/20260210/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=...

        let components = authHeader.split(separator: ",", omittingEmptySubsequences: true)
        guard components.count >= 3 else { return false }

        // Extract values
        var credentialPart = ""
        var signedHeadersPart = ""
        var signaturePart = ""

        for component in components {
            let trimmed = component.trimmingCharacters(in: .whitespaces)
            if trimmed.starts(with: "Credential=") {
                credentialPart = trimmed
            } else if trimmed.starts(with: "SignedHeaders=") {
                signedHeadersPart = trimmed
            } else if trimmed.starts(with: "Signature=") {
                signaturePart = trimmed
            }
        }

        guard let credentialRange = credentialPart.range(of: "Credential="),
            let signedHeadersRange = signedHeadersPart.range(of: "SignedHeaders="),
            let signatureRange = signaturePart.range(of: "Signature=")
        else {
            return false
        }

        let credential = String(credentialPart[credentialRange.upperBound...])
        let signedHeaders = String(signedHeadersPart[signedHeadersRange.upperBound...])
        let signature = String(signaturePart[signatureRange.upperBound...])

        // Credential Parts
        let credParts = credential.split(separator: "/")
        guard credParts.count == 5 else { return false }
        let accessKeyID = String(credParts[0])
        let dateStamp = String(credParts[1])
        let region = String(credParts[2])
        let service = String(credParts[3])

        guard accessKeyID == self.accessKey else { return false }  // Check Access Key

        // 1. Canonical Request
        let method = request.method.rawValue
        let uri = request.uri.path
        // Query must be sorted by key
        // query must be sorted by key
        let query = request.uri.queryParameters.map { "\($0.key)=\($0.value)" }.sorted().joined(
            separator: "&")

        // Canonical Headers
        // Note: Headers must be lowercased in keys.
        let headersToSign = signedHeaders.split(separator: ";").map { String($0) }
        var canonicalHeaders = ""
        for headerName in headersToSign {
            var value: String = ""
            if let fieldName = HTTPField.Name(headerName), let v = request.headers[fieldName] {
                value = v
            } else if headerName == "host", let host = request.uri.host {
                value = host
                if let port = request.uri.port {
                    value += ":\(port)"
                }
            }

            // AWS Signature V4 requires all signed headers to be present in Canonical Headers
            canonicalHeaders += "\(headerName):\(value)\n"
        }

        let payloadHash =
            request.headers[HTTPField.Name("x-amz-content-sha256")!] ?? "UNSIGNED-PAYLOAD"

        let canonicalRequest = """
            \(method)
            \(uri)
            \(query)
            \(canonicalHeaders)
            \(signedHeaders)
            \(payloadHash)
            """

        // 2. String to Sign
        let algorithm = "AWS4-HMAC-SHA256"
        let requestDateTime = request.headers[HTTPField.Name("x-amz-date")!] ?? ""
        let scope = "\(dateStamp)/\(region)/\(service)/aws4_request"

        let canonicalRequestHash = SHA256.hash(data: Data(canonicalRequest.utf8)).map {
            String(format: "%02x", $0)
        }.joined()

        let stringToSign = """
            \(algorithm)
            \(requestDateTime)
            \(scope)
            \(canonicalRequestHash)
            """

        // 3. Calculation
        // Signing Key
        let kSecret = "AWS4" + secretKey
        let kDate = try hmac(key: Data(kSecret.utf8), data: Data(dateStamp.utf8))
        let kRegion = try hmac(key: kDate, data: Data(region.utf8))
        let kService = try hmac(key: kRegion, data: Data(service.utf8))
        let kSigning = try hmac(key: kService, data: Data("aws4_request".utf8))

        let calculatedSignatureData = try hmac(key: kSigning, data: Data(stringToSign.utf8))
        let calculatedSignature = calculatedSignatureData.map { String(format: "%02x", $0) }
            .joined()

        return signature == calculatedSignature
    }

    private func hmac(key: Data, data: Data) throws -> Data {
        let auth = HMAC<SHA256>.authenticationCode(for: data, using: SymmetricKey(data: key))
        return Data(auth)
    }
}
