import Crypto
import Foundation
import HTTPTypes
import Hummingbird

/// Middleware for authenticating S3 API requests using AWS signature version 4.
/// Supports both header-based and query parameter-based authentication.
/// Validates request signatures against stored user credentials to ensure request integrity.
///
/// Authentication flow:
/// 1. Extract credentials from Authorization header or query parameters
/// 2. Verify signature using AWS Signature Version 4 algorithm
/// 3. Set authenticated principal in request context
/// 4. Allow request to proceed or throw authentication error
struct S3Authenticator: RouterMiddleware {
    typealias Context = S3RequestContext
    let userStore: UserStore

    init(userStore: UserStore) {
        self.userStore = userStore
    }

    /// Processes authentication for incoming requests.
    /// Determines authentication method (header vs query params) and validates credentials.
    /// Prevents multiple authentication methods and ensures proper signature verification.
    ///
    /// - Parameters:
    ///   - request: The incoming HTTP request
    ///   - context: S3 request context to be updated with authenticated principal
    ///   - next: Next middleware/router in the chain
    /// - Returns: Response from downstream handlers if authentication succeeds
    /// - Throws: S3Error for authentication failures or invalid request format
    func handle(_ request: Input, context: Context, next: (Input, Context) async throws -> Output)
        async throws -> Output
    {
        let hasAuthHeader =
            request.headers[.authorization]?.starts(with: "AWS4-HMAC-SHA256") ?? false
        let hasQueryAuth = request.uri.queryParameters.get("X-Amz-Algorithm") == "AWS4-HMAC-SHA256"

        if hasAuthHeader && hasQueryAuth {
            throw S3Error.invalidArgument  // Multiple authentication methods
        }

        if hasAuthHeader {
            if let authHeader = request.headers[.authorization] {
                if let accessKey = try await verifyHeaderSignature(
                    request: request, authHeader: authHeader)
                {
                    var context = context
                    context.principal = accessKey
                    return try await next(request, context)
                } else {
                    throw S3Error.signatureDoesNotMatch
                }
            }
        }

        if hasQueryAuth {
            if let accessKey = try await verifyQuerySignature(request: request) {
                var context = context
                context.principal = accessKey
                return try await next(request, context)
            } else {
                throw S3Error.signatureDoesNotMatch
            }
        }

        return try await next(request, context)
    }

    /// Verifies AWS Signature Version 4 authentication from Authorization header.
    /// Parses the authorization header, reconstructs the canonical request, and validates
    /// the signature against the stored user secret key.
    ///
    /// - Parameters:
    ///   - request: The HTTP request with headers to verify
    ///   - authHeader: The Authorization header value containing AWS4 credentials
    /// - Returns: Access key ID if authentication succeeds, nil otherwise
    /// - Throws: S3Error for invalid signatures or missing credentials
    func verifyHeaderSignature(request: Request, authHeader: String) async throws -> String? {
        let components = authHeader.split(separator: ",", omittingEmptySubsequences: true)
        guard components.count >= 3 else { return nil }

        var credentialPart = ""
        var signedHeadersPart = ""
        var signaturePart = ""

        for component in components {
            let trimmed = component.trimmingCharacters(in: .whitespaces)
            if trimmed.contains("Credential=") {
                credentialPart = trimmed
            } else if trimmed.contains("SignedHeaders=") {
                signedHeadersPart = trimmed
            } else if trimmed.contains("Signature=") {
                signaturePart = trimmed
            }
        }

        guard let credentialRange = credentialPart.range(of: "Credential="),
            let signedHeadersRange = signedHeadersPart.range(of: "SignedHeaders="),
            let signatureRange = signaturePart.range(of: "Signature=")
        else {
            return nil
        }

        let credential = String(credentialPart[credentialRange.upperBound...])
        let signedHeaders = String(signedHeadersPart[signedHeadersRange.upperBound...])
        let signature = String(signaturePart[signatureRange.upperBound...])

        let credParts = credential.split(separator: "/")
        guard credParts.count == 5 else { return nil }
        let accessKeyID = String(credParts[0])
        let dateStamp = String(credParts[1])
        let region = String(credParts[2])
        let service = String(credParts[3])

        guard let user = try await userStore.getUser(accessKey: accessKeyID) else {
            return nil
        }
        let secretKey = user.secretKey

        let method = request.method.rawValue
        let uri = request.uri.path
        let query = request.uri.queryParameters.map { "\($0.key)=\($0.value)" }.sorted().joined(
            separator: "&")

        let headersToSign = signedHeaders.split(separator: ";").map { String($0) }
        var canonicalHeaders = ""
        for headerName in headersToSign {
            var value: String = ""
            if headerName == "host" {
                // Use "host" header if available, manually constructed Name
                if let v = request.headers[HTTPField.Name("host")!] {
                    value = v
                }
            } else if let fieldName = HTTPField.Name(headerName), let v = request.headers[fieldName]
            {
                value = v
            }
            canonicalHeaders += "\(headerName.lowercased()):\(value)\n"
        }

        let signedHeadersString = signedHeaders
        let payloadHash =
            request.headers[HTTPField.Name("x-amz-content-sha256")!] ?? "UNSIGNED-PAYLOAD"

        let canonicalRequest = """
            \(method)
            \(uri)
            \(query)
            \(canonicalHeaders)
            \(signedHeadersString)
            \(payloadHash)
            """

        let algorithm = "AWS4-HMAC-SHA256"
        let credentialScope = "\(dateStamp)/\(region)/\(service)/aws4_request"

        let requestDate: String
        if let xAmzDate = request.headers[HTTPField.Name("x-amz-date")!] {
            requestDate = xAmzDate
        } else if let date = request.headers[.date] {
            requestDate = date
        } else {
            requestDate = ""
        }

        let stringToSign = """
            \(algorithm)
            \(requestDate)
            \(credentialScope)
            \(SHA256.hash(data: Data(canonicalRequest.utf8)).hexString)
            """

        let kDate = try HMAC256.compute(dateStamp, key: "AWS4" + secretKey)
        let kRegion = try HMAC256.compute(region, key: kDate)
        let kService = try HMAC256.compute(service, key: kRegion)
        let kSigning = try HMAC256.compute("aws4_request", key: kService)
        let calculatedSignature = try HMAC256.compute(stringToSign, key: kSigning).hexString

        if calculatedSignature == signature {
            return accessKeyID
        } else {
            return nil
        }
    }

    /// Verifies AWS Signature Version 4 authentication from query parameters.
    /// Used for presigned URLs where credentials are passed as query parameters.
    /// Reconstructs the canonical request and validates the signature.
    ///
    /// - Parameter request: The HTTP request with query parameters containing credentials
    /// - Returns: Access key ID if authentication succeeds, nil otherwise
    /// - Throws: S3Error for invalid signatures or missing parameters
    func verifyQuerySignature(request: Request) async throws -> String? {
        let params = request.uri.queryParameters
        guard let algorithm = params.get("X-Amz-Algorithm"),
            let credential = params.get("X-Amz-Credential"),
            let date = params.get("X-Amz-Date"),
            let signedHeaders = params.get("X-Amz-SignedHeaders"),
            let signature = params.get("X-Amz-Signature")
        else {
            return nil
        }

        let credParts = credential.split(separator: "/")
        guard credParts.count == 5 else { return nil }
        let accessKeyID = String(credParts[0])
        let dateStamp = String(credParts[1])
        let region = String(credParts[2])
        let service = String(credParts[3])

        guard let user = try await userStore.getUser(accessKey: accessKeyID) else {
            return nil
        }
        let secretKey = user.secretKey

        let method = request.method.rawValue
        let uri = request.uri.path

        // In Query Auth, all X-Amz parameters EXCEPT X-Amz-Signature should be in canonical query
        let query = params.filter { $0.key != "X-Amz-Signature" }
            .map { "\($0.key)=\($0.value)" }
            .sorted()
            .joined(separator: "&")

        let headersToSign = signedHeaders.split(separator: ";").map { String($0) }
        var canonicalHeaders = ""
        for headerName in headersToSign {
            var value: String = ""
            if headerName == "host" {
                if let v = request.headers[HTTPField.Name("host")!] {
                    value = v
                }
            } else if let fieldName = HTTPField.Name(headerName), let v = request.headers[fieldName]
            {
                value = v
            }
            canonicalHeaders += "\(headerName.lowercased()):\(value)\n"
        }

        let payloadHash = "UNSIGNED-PAYLOAD"

        let canonicalRequest = """
            \(method)
            \(uri)
            \(query)
            \(canonicalHeaders)
            \(signedHeaders)
            \(payloadHash)
            """

        let credentialScope = "\(dateStamp)/\(region)/\(service)/aws4_request"

        let stringToSign = """
            \(algorithm)
            \(date)
            \(credentialScope)
            \(SHA256.hash(data: Data(canonicalRequest.utf8)).hexString)
            """

        let kDate = try HMAC256.compute(dateStamp, key: "AWS4" + secretKey)
        let kRegion = try HMAC256.compute(region, key: kDate)
        let kService = try HMAC256.compute(service, key: kRegion)
        let kSigning = try HMAC256.compute("aws4_request", key: kService)
        let calculatedSignature = try HMAC256.compute(stringToSign, key: kSigning).hexString

        if calculatedSignature == signature {
            // Check Expiration
            if let expiresString = params.get("X-Amz-Expires"), let expires = Int(expiresString) {
                // Parse date (YYYYMMDDTHHMMSSZ)
                let formatter = DateFormatter()
                formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
                formatter.timeZone = TimeZone(secondsFromGMT: 0)
                if let requestDate = formatter.date(from: date) {
                    if Date().timeIntervalSince(requestDate) > Double(expires) {
                        throw S3Error.expiredToken  // Token expired
                    }
                }
            }
            return accessKeyID
        } else {
            return nil
        }
    }
}

enum HMAC256 {
    /// Computes HMAC-SHA256 signature for a message using a key
    /// - Parameters:
    ///   - message: The message to sign
    ///   - key: The key data for HMAC computation
    /// - Returns: HMAC-SHA256 signature as Data
    /// - Throws: Crypto errors if computation fails
    static func compute(_ message: String, key: Data) throws -> Data {
        let symmetricKey = SymmetricKey(data: key)
        let signature = HMAC<SHA256>.authenticationCode(
            for: Data(message.utf8), using: symmetricKey)
        return Data(signature)
    }

    /// Computes HMAC-SHA256 signature for a message using a string key
    /// - Parameters:
    ///   - message: The message to sign
    ///   - key: The key string for HMAC computation
    /// - Returns: HMAC-SHA256 signature as Data
    /// - Throws: Crypto errors if computation fails
    static func compute(_ message: String, key: String) throws -> Data {
        let symmetricKey = SymmetricKey(data: Data(key.utf8))
        let signature = HMAC<SHA256>.authenticationCode(
            for: Data(message.utf8), using: symmetricKey)
        return Data(signature)
    }
}

extension Data {
    var hexString: String {
        map { String(format: "%02x", $0) }.joined()
    }
}

extension SHA256.Digest {
    var hexString: String {
        map { String(format: "%02x", $0) }.joined()
    }
}
