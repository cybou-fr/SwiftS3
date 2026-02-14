import Hummingbird

/// Represents an S3 API error with a specific error code, message, and HTTP status.
/// Provides XML serialization for error responses.
struct S3Error: Error, @unchecked Sendable, Equatable {
    let code: String
    let message: String
    let statusCode: HTTPResponse.Status

    var xml: String {
        return """
            <?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>\(code)</Code>
                <Message>\(message)</Message>
            </Error>
            """
    }

    static let methodNotAllowed = S3Error(
        code: "MethodNotAllowed",
        message: "The specified method is not allowed against this resource.",
        statusCode: .methodNotAllowed)
    static let noSuchBucket = S3Error(
        code: "NoSuchBucket", message: "The specified bucket does not exist.", statusCode: .notFound
    )
    static let noSuchKey = S3Error(
        code: "NoSuchKey", message: "The specified key does not exist.", statusCode: .notFound)
    static let bucketAlreadyExists = S3Error(
        code: "BucketAlreadyExists", message: "The requested bucket name is not available.",
        statusCode: .conflict)
    static let invalidBucketName = S3Error(
        code: "InvalidBucketName", message: "The specified bucket is not valid.", statusCode: .badRequest)
    static let bucketNotEmpty = S3Error(
        code: "BucketNotEmpty", message: "The bucket you tried to delete is not empty.",
        statusCode: .conflict)
    static let internalError = S3Error(
        code: "InternalError", message: "We encountered an internal error. Please try again.",
        statusCode: .internalServerError)
    static let signatureDoesNotMatch = S3Error(
        code: "SignatureDoesNotMatch",
        message:
            "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
        statusCode: .forbidden)
    static let noSuchUpload = S3Error(
        code: "NoSuchUpload",
        message:
            "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
        statusCode: .notFound)
    static let invalidPart = S3Error(
        code: "InvalidPart",
        message:
            "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
        statusCode: .badRequest)
    static let invalidRequest = S3Error(
        code: "InvalidRequest",
        message: "The request is invalid.",
        statusCode: .badRequest)
    static let invalidAccessKeyId = S3Error(
        code: "InvalidAccessKeyId",
        message: "The AWS Access Key Id you provided does not exist in our records.",
        statusCode: .forbidden)
    static let accessDenied = S3Error(
        code: "AccessDenied", message: "Access Denied", statusCode: .forbidden)
    static let badDigest = S3Error(
        code: "BadDigest",
        message:
            "The Content-MD5 or checksum value that you specified did not match what the server received.",
        statusCode: .badRequest)
    static let xAmzContentSHA256Mismatch = S3Error(
        code: "XAmzContentSHA256Mismatch",
        message: "The provided 'x-amz-content-sha256' header does not match what was computed.",
        statusCode: .badRequest)

    static let noSuchBucketPolicy = S3Error(
        code: "NoSuchBucketPolicy", message: "The bucket policy does not exist",
        statusCode: .notFound)

    static let malformedPolicy = S3Error(
        code: "MalformedPolicy", message: "Policy has invalid resource", statusCode: .badRequest)

    static let notImplemented = S3Error(
        code: "NotImplemented",
        message: "A header you provided implies functionality that is not implemented",
        statusCode: .notImplemented)

    static let invalidArgument = S3Error(
        code: "InvalidArgument",
        message: "Invalid Argument",
        statusCode: .badRequest)

    static let expiredToken = S3Error(
        code: "ExpiredToken",
        message: "The provided token has expired.",
        statusCode: .forbidden)

    static let noSuchLifecycleConfiguration = S3Error(
        code: "NoSuchLifecycleConfiguration",
        message: "The lifecycle configuration does not exist.",
        statusCode: .notFound)

    static let invalidEncryption = S3Error(
        code: "InvalidEncryption",
        message: "The encryption parameters are invalid or missing.",
        statusCode: .badRequest)
}

extension S3Error: ResponseGenerator {
    /// Generates an HTTP response for this S3 error.
    /// - Parameters:
    ///   - request: The incoming HTTP request that caused the error
    ///   - context: The request context
    /// - Returns: An HTTP response with XML error details
    public func response(from request: Request, context: some RequestContext) throws -> Response {
        let headers: HTTPFields = [
            .contentType: "application/xml"
        ]
        return Response(
            status: self.statusCode, headers: headers,
            body: .init(byteBuffer: ByteBuffer(string: self.xml)))
    }
}

struct S3ErrorMiddleware<Context: RequestContext>: RouterMiddleware {
    /// Handles incoming requests and converts S3Error exceptions to proper HTTP error responses.
    /// - Parameters:
    ///   - request: The incoming HTTP request
    ///   - context: The request context
    ///   - next: The next middleware in the chain
    /// - Returns: The response from the next middleware, or an error response if an S3Error was thrown
    func handle(_ request: Input, context: Context, next: (Input, Context) async throws -> Output)
        async throws -> Output
    {
        do {
            return try await next(request, context)
        } catch let error as S3Error {
            return try error.response(from: request, context: context)
        }
    }
}
