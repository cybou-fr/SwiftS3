import Crypto
import Foundation
import HTTPTypes
import Hummingbird

struct S3Authenticator: RouterMiddleware {
    typealias Context = S3RequestContext
    let userStore: UserStore

    init(userStore: UserStore) {
        self.userStore = userStore
    }

    func handle(_ request: Input, context: Context, next: (Input, Context) async throws -> Output)
        async throws -> Output
    {
        guard let authHeader = request.headers[.authorization] else {
            return try await next(request, context)
        }

        guard authHeader.starts(with: "AWS4-HMAC-SHA256") else {
            return try await next(request, context)
        }

        if let accessKey = try await verifySignature(request: request, authHeader: authHeader) {
            var context = context
            context.principal = accessKey
            return try await next(request, context)
        } else {
            throw S3Error.signatureDoesNotMatch
        }
    }

    func verifySignature(request: Request, authHeader: String) async throws -> String? {
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
}

enum HMAC256 {
    static func compute(_ message: String, key: Data) throws -> Data {
        let symmetricKey = SymmetricKey(data: key)
        let signature = HMAC<SHA256>.authenticationCode(
            for: Data(message.utf8), using: symmetricKey)
        return Data(signature)
    }

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
