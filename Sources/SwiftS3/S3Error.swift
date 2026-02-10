import Hummingbird

struct S3Error: Error {
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
}

extension S3Error: ResponseGenerator {
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
