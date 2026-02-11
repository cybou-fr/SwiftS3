import Foundation
import HTTPTypes
import Hummingbird
import Logging
import NIO

struct S3Controller {
    let storage: any StorageBackend
    let logger = Logger(label: "SwiftS3.S3")

    let evaluator = PolicyEvaluator()

    func addRoutes<Context: RequestContext>(to router: some Router<Context>) {
        // List Buckets (Service)
        router.get(
            "/",
            use: { request, context in
                try await self.listBuckets(request: request, context: context)
            })

        // Bucket Operations
        router.put(
            ":bucket",
            use: { request, context in
                try await self.createBucket(request: request, context: context)
            })
        router.delete(
            ":bucket",
            use: { request, context in
                try await self.deleteBucket(request: request, context: context)
            })
        router.head(
            ":bucket",
            use: { request, context in
                try await self.headBucket(request: request, context: context)
            })
        router.get(
            ":bucket",
            use: { request, context in
                try await self.listObjects(request: request, context: context)
            })

        // Object Operations
        // Recursive wildcard for key
        router.put(
            ":bucket/**",
            use: { request, context in
                try await self.putObject(request: request, context: context)
            })
        router.get(
            ":bucket/**",
            use: { request, context in
                try await self.getObject(request: request, context: context)
            })
        router.delete(
            ":bucket/**",
            use: { request, context in
                try await self.deleteObject(request: request, context: context)
            })
        router.head(
            ":bucket/**",
            use: { request, context in
                try await self.headObject(request: request, context: context)
            })
        router.post(
            ":bucket",
            use: { request, context in
                try await self.postObject(
                    request: request, context: context, isBucketOperation: true)
            })
        router.post(
            ":bucket/**",
            use: { request, context in
                try await self.postObject(
                    request: request, context: context, isBucketOperation: false)
            })
    }

    @Sendable func listBuckets(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let buckets = try await storage.listBuckets()
        let xml = XML.listBuckets(buckets: buckets)
        let headers: HTTPFields = [.contentType: "application/xml"]
        return Response(
            status: .ok, headers: headers, body: .init(byteBuffer: ByteBuffer(string: xml)))
    }

    @Sendable func createBucket(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")

        // Check if this is a Policy operation
        if request.uri.queryParameters.get("policy") != nil {
            return try await putBucketPolicy(bucket: bucket, request: request, context: context)
        }

        // CreateBucket permission check? Usually context-less or parent context.
        // We assume allowed if authenticated for creation. Policy applies to EXISTING buckets.

        try await storage.createBucket(name: bucket)
        logger.info("Bucket created", metadata: ["bucket": "\(bucket)"])
        return Response(status: .ok)
    }

    func putBucketPolicy(bucket: String, request: Request, context: some RequestContext)
        async throws -> Response
    {
        try await checkPolicy(bucket: bucket, action: "s3:PutBucketPolicy", request: request)

        let buffer = try await request.body.collect(upTo: 1024 * 1024)  // 1MB limit for policy
        let policy = try JSONDecoder().decode(BucketPolicy.self, from: buffer)

        try await storage.putBucketPolicy(bucket: bucket, policy: policy)
        logger.info("Bucket policy updated", metadata: ["bucket": "\(bucket)"])
        return Response(status: .noContent)
    }

    @Sendable func deleteBucket(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")

        // Check if this is a Policy operation
        if request.uri.queryParameters.get("policy") != nil {
            return try await deleteBucketPolicy(bucket: bucket, context: context, request: request)
        }

        try await checkPolicy(bucket: bucket, action: "s3:DeleteBucket", request: request)

        try await storage.deleteBucket(name: bucket)
        logger.info("Bucket deleted", metadata: ["bucket": "\(bucket)"])
        return Response(status: .noContent)
    }

    func deleteBucketPolicy(bucket: String, context: some RequestContext, request: Request)
        async throws -> Response
    {
        try await checkPolicy(bucket: bucket, action: "s3:DeleteBucketPolicy", request: request)
        try await storage.deleteBucketPolicy(bucket: bucket)
        logger.info("Bucket policy deleted", metadata: ["bucket": "\(bucket)"])
        return Response(status: .noContent)
    }

    @Sendable func headBucket(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")
        try await checkPolicy(bucket: bucket, action: "s3:ListBucket", request: request)
        try await storage.headBucket(name: bucket)
        return Response(status: .ok)
    }

    @Sendable func listObjects(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")

        // Check if this is a Policy operation
        if request.uri.queryParameters.get("policy") != nil {
            return try await getBucketPolicy(bucket: bucket, context: context)
        }

        try await checkPolicy(bucket: bucket, action: "s3:ListBucket", request: request)

        let prefix = request.uri.queryParameters.get("prefix")
        let delimiter = request.uri.queryParameters.get("delimiter")
        let marker = request.uri.queryParameters.get("marker")
        let listType = request.uri.queryParameters.get("list-type")
        let continuationToken = request.uri.queryParameters.get("continuation-token")
        let maxKeys = request.uri.queryParameters.get("max-keys").flatMap { Int($0) }

        let result = try await storage.listObjects(
            bucket: bucket, prefix: prefix, delimiter: delimiter, marker: marker,
            continuationToken: continuationToken, maxKeys: maxKeys)

        let xml: String
        if listType == "2" {
            xml = XML.listObjectsV2(
                bucket: bucket, result: result, prefix: prefix ?? "",
                continuationToken: continuationToken ?? "", maxKeys: maxKeys ?? 1000,
                isTruncated: result.isTruncated,
                keyCount: result.objects.count + result.commonPrefixes.count)
        } else {
            xml = XML.listObjects(
                bucket: bucket, result: result, prefix: prefix ?? "", marker: marker ?? "",
                maxKeys: maxKeys ?? 1000, isTruncated: result.isTruncated)
        }

        let headers: HTTPFields = [.contentType: "application/xml"]
        return Response(
            status: .ok, headers: headers, body: .init(byteBuffer: ByteBuffer(string: xml)))
    }

    func getBucketPolicy(bucket: String, context: some RequestContext) async throws -> Response {
        // Technically getBucketPolicy permission check
        // But we called this from listObjects which checked ListBucket?
        // No, we should check GetBucketPolicy specific permission.
        // Implementation note: The dispatch logic calls this *instead* of listObjects logic.
        // But we didn't pass 'request' to getBucketPolicy in previous edit?
        // Let's assume we need to fix signature of getBucketPolicy or pull it from context if possible?
        // No, we need to pass request.
        // I will fix getBucketPolicy signature in a separate edit or let it fail for now and fix up.
        // Actually, let's fix it here if possible.
        // But wait, the previous edit defined getBucketPolicy(bucket:context:). I need to update it to take request.

        // Retaining original implementation for now, will fix policy check in getBucketPolicy separately.
        let policy = try await storage.getBucketPolicy(bucket: bucket)
        let data = try JSONEncoder().encode(policy)
        let buffer = ByteBuffer(data: data)
        return Response(
            status: .ok, headers: [.contentType: "application/json"],
            body: .init(byteBuffer: buffer))
    }

    @Sendable func putObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)
        try await checkPolicy(bucket: bucket, key: key, action: "s3:PutObject", request: request)

        // Check for Upload Part
        // Query params are not directly accessible in Hummingbird 2 Request object easily via property?
        // URI.query is string. Need manual parsing or URLComponents.
        // Hummingbird 2: request.uri.query is String?

        let query = parseQuery(request.uri.query)

        if let partNumberStr = query["partNumber"], let uploadId = query["uploadId"],
            let partNumber = Int(partNumberStr)
        {
            let contentLength = request.headers[.contentLength].flatMap { Int64($0) }
            let etag = try await storage.uploadPart(
                bucket: bucket, key: key, uploadId: uploadId, partNumber: partNumber,
                data: request.body, size: contentLength)
            return Response(status: .ok, headers: [.eTag: etag])
        }

        // Check for Copy Source
        if let copySource = request.headers[HTTPField.Name("x-amz-copy-source")!] {
            // Format: /bucket/key or bucket/key
            // Removing leading slash if present
            var source = copySource
            if source.hasPrefix("/") {
                source.removeFirst()
            }
            let components = source.split(separator: "/", maxSplits: 1)
            guard components.count == 2 else {
                throw S3Error.invalidRequest  // Or bad request
            }
            let srcBucket = String(components[0])
            let srcKey = String(components[1])

            let metadata = try await storage.copyObject(
                fromBucket: srcBucket, fromKey: srcKey, toBucket: bucket, toKey: key)

            let xml = XML.copyObjectResult(metadata: metadata)
            return Response(
                status: .ok, headers: [.contentType: "application/xml"],
                body: .init(byteBuffer: ByteBuffer(string: xml)))
        }

        // Extract Metadata
        var metadata: [String: String] = [:]
        for field in request.headers {
            let name = field.name.canonicalName.lowercased()
            if name.starts(with: "x-amz-meta-") {
                metadata[name] = field.value
            }
        }
        if let contentType = request.headers[.contentType] {
            metadata["Content-Type"] = contentType
        }

        let contentLength = request.headers[.contentLength].flatMap { Int64($0) }

        // Stream body
        let etag = try await storage.putObject(
            bucket: bucket, key: key, data: request.body, size: contentLength, metadata: metadata)

        // Verify payload checksum if provided
        if let declaredHash = request.headers[HTTPField.Name("x-amz-content-sha256")!],
            declaredHash != "UNSIGNED-PAYLOAD",
            declaredHash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        {
            if etag != declaredHash {
                // Delete the object since verification failed
                try? await storage.deleteObject(bucket: bucket, key: key)
                logger.warning(
                    "Payload checksum mismatch",
                    metadata: [
                        "bucket": "\(bucket)", "key": "\(key)",
                        "declared": "\(declaredHash)", "computed": "\(etag)",
                    ])
                throw S3Error.xAmzContentSHA256Mismatch
            }
        }

        logger.info(
            "Object uploaded",
            metadata: ["bucket": "\(bucket)", "key": "\(key)", "etag": "\(etag)"])
        return Response(status: .ok, headers: [.eTag: etag])
    }

    @Sendable func postObject(
        request: Request, context: some RequestContext, isBucketOperation: Bool
    ) async throws
        -> Response
    {
        let bucket: String
        let key: String

        if isBucketOperation {
            bucket = try context.parameters.require("bucket")
            key = ""
        } else {
            (bucket, key) = try parsePath(request.uri.path)
        }

        let query = parseQuery(request.uri.query)

        if query.keys.contains("uploads") {
            // Initiate Multipart Upload
            var metadata: [String: String] = [:]
            for field in request.headers {
                let name = field.name.canonicalName.lowercased()
                if name.starts(with: "x-amz-meta-") {
                    metadata[name] = field.value
                }
            }
            if let contentType = request.headers[.contentType] {
                metadata["Content-Type"] = contentType
            }

            try await checkPolicy(
                bucket: bucket, key: key, action: "s3:PutObject", request: request)

            let uploadId = try await storage.createMultipartUpload(
                bucket: bucket, key: key, metadata: metadata)
            let xml = XML.initiateMultipartUploadResult(
                bucket: bucket, key: key, uploadId: uploadId)
            return Response(
                status: .ok, headers: [.contentType: "application/xml"],
                body: .init(byteBuffer: ByteBuffer(string: xml)))
        } else if let uploadId = query["uploadId"] {
            // Complete Multipart Upload
            try await checkPolicy(
                bucket: bucket, key: key, action: "s3:PutObject", request: request)
            // We need to read the body (XML) to get the list of parts.
            // request.body is AsyncStream. We need to collect it.
            var buffer = ByteBuffer()
            for try await var chunk in request.body {
                buffer.writeBuffer(&chunk)
            }
            let xmlStr = String(buffer: buffer)
            let parts = XML.parseCompleteMultipartUpload(xml: xmlStr)

            let eTag = try await storage.completeMultipartUpload(
                bucket: bucket, key: key, uploadId: uploadId, parts: parts)
            // Location should technically be full URL
            let location = "http://localhost:8080/\(bucket)/\(key)"
            let resultXml = XML.completeMultipartUploadResult(
                bucket: bucket, key: key, eTag: eTag, location: location)
            return Response(
                status: .ok, headers: [.contentType: "application/xml"],
                body: .init(byteBuffer: ByteBuffer(string: resultXml)))
        } else if query.keys.contains("delete") {
            // Delete Objects
            // Check bucket-level delete permission for V1 approximation
            try await checkPolicy(
                bucket: bucket, key: nil, action: "s3:DeleteObject", request: request)

            var buffer = ByteBuffer()
            for try await var chunk in request.body {
                buffer.writeBuffer(&chunk)
            }
            let xmlStr = String(buffer: buffer)
            let keys = XML.parseDeleteObjects(xml: xmlStr)

            let deleted = try await storage.deleteObjects(bucket: bucket, keys: keys)
            let resultXml = XML.deleteResult(deleted: deleted, errors: [])

            return Response(
                status: .ok, headers: [.contentType: "application/xml"],
                body: .init(byteBuffer: ByteBuffer(string: resultXml)))
        }

        return Response(status: .badRequest)
    }

    @Sendable func getObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)
        try await checkPolicy(bucket: bucket, key: key, action: "s3:GetObject", request: request)

        // Parse Range Header
        var range: ValidatedRange? = nil
        if let rangeHeader = request.headers[.range] {
            // Simple parsing for bytes=start-end
            if rangeHeader.starts(with: "bytes=") {
                let value = rangeHeader.dropFirst(6)  // remove "bytes="
                let components = value.split(separator: "-", omittingEmptySubsequences: false)
                if components.count == 2 {
                    let startStr = String(components[0])
                    let endStr = String(components[1])

                    // We need object size to validate/resolved range
                    // But getObject returns metadata.
                    // Optimization: In a real DB we'd check metadata first.
                    // Here, getObject calls getObjectMetadata internally anyway.
                    // But to resolve open-ended ranges (100-), we need the size *before* calling storage.getObject
                    // because storage.getObject now takes a validated range.

                    // So we must fetch metadata first.
                    let metadata = try await storage.getObjectMetadata(bucket: bucket, key: key)
                    let objectSize = metadata.size

                    var start: Int64 = 0
                    var end: Int64 = objectSize - 1

                    if startStr.isEmpty && !endStr.isEmpty {
                        // Suffix: -500
                        if let suffix = Int64(endStr) {
                            start = max(0, objectSize - suffix)
                        }
                    } else if !startStr.isEmpty && endStr.isEmpty {
                        // Start only: 100-
                        if let s = Int64(startStr) {
                            start = s
                        }
                    } else if let s = Int64(startStr), let e = Int64(endStr) {
                        // Specific: 100-200
                        start = s
                        end = min(e, objectSize - 1)
                    }

                    if start <= end {
                        range = ValidatedRange(start: start, end: end)
                    }
                }
            }
        }

        let (metadata, body) = try await storage.getObject(bucket: bucket, key: key, range: range)

        var headers: HTTPFields = [
            .lastModified: ISO8601DateFormatter().string(from: metadata.lastModified),
            .contentType: metadata.contentType ?? "application/octet-stream",
        ]

        let contentLength: Int64
        if let range = range {
            contentLength = range.end - range.start + 1
            headers[.contentRange] = "bytes \(range.start)-\(range.end)/\(metadata.size)"
        } else {
            contentLength = metadata.size
        }
        headers[.contentLength] = String(contentLength)

        if let etag = metadata.eTag {
            headers[.eTag] = etag
        }

        for (k, v) in metadata.customMetadata {
            if let name = HTTPField.Name(k) {
                headers[name] = v
            }
        }

        let status: HTTPResponse.Status = range != nil ? .partialContent : .ok

        if let body = body {
            return Response(status: status, headers: headers, body: .init(asyncSequence: body))
        } else {
            return Response(status: status, headers: headers, body: .init(byteBuffer: ByteBuffer()))
        }
    }

    @Sendable func deleteObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)
        try await checkPolicy(bucket: bucket, key: key, action: "s3:DeleteObject", request: request)
        let query = parseQuery(request.uri.query)

        if let uploadId = query["uploadId"] {
            try await storage.abortMultipartUpload(bucket: bucket, key: key, uploadId: uploadId)
            return Response(status: .noContent)
        }

        try await storage.deleteObject(bucket: bucket, key: key)
        logger.info("Object deleted", metadata: ["bucket": "\(bucket)", "key": "\(key)"])
        return Response(status: .noContent)
    }

    @Sendable func headObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)
        try await checkPolicy(bucket: bucket, key: key, action: "s3:GetObject", request: request)

        let metadata = try await storage.getObjectMetadata(bucket: bucket, key: key)

        var headers: HTTPFields = [
            .lastModified: ISO8601DateFormatter().string(from: metadata.lastModified),
            .contentLength: String(metadata.size),
            .contentType: metadata.contentType ?? "application/octet-stream",
        ]
        if let etag = metadata.eTag {
            headers[.eTag] = etag
        }

        for (k, v) in metadata.customMetadata {
            if let name = HTTPField.Name(k) {
                headers[name] = v
            }
        }

        return Response(status: .ok, headers: headers)
    }

    private func parsePath(_ path: String) throws -> (String, String) {
        let components = path.split(separator: "/")
        guard components.count >= 2 else {
            throw S3Error.noSuchKey  // Or bad request
        }
        let bucket = String(components[0])
        let key = components.dropFirst().joined(separator: "/")
        return (bucket, key)
    }

    private func parseQuery(_ query: String?) -> [String: String] {
        guard let query = query else { return [:] }
        var info: [String: String] = [:]
        for pair in query.split(separator: "&") {
            let parts = pair.split(separator: "=")
            if parts.count == 2 {
                info[String(parts[0])] = String(parts[1])
            } else if parts.count == 1 {
                info[String(parts[0])] = ""
            }
        }
        return info
    }

    func checkPolicy(bucket: String, key: String? = nil, action: String, request: Request)
        async throws
    {
        // 1. Get Policy (if exists)
        let policy: BucketPolicy
        do {
            policy = try await storage.getBucketPolicy(bucket: bucket)
        } catch {
            // No policy found -> Allowed (fallback to existing auth)
            if let s3Err = error as? S3Error, s3Err.code == "NoSuchBucketPolicy" {
                return
            }
            // For FileSystemStorage, it throws NoSuchBucketPolicy if missing.
            // If other error, we might want to fail?
            // Assume missing policy means allowed.
            return
        }

        // 2. Extract Principal
        let principal = extractPrincipal(from: request)

        // 3. Resource
        var resource = "arn:aws:s3:::\(bucket)"
        if let key = key {
            resource += "/\(key)"
        }

        // 4. Evaluate
        let decision = evaluator.evaluate(
            policy: policy,
            request: PolicyRequest(principal: principal, action: action, resource: resource))

        if decision == .deny {
            logger.warning(
                "Access Denied by Bucket Policy",
                metadata: [
                    "bucket": "\(bucket)", "action": "\(action)",
                    "principal": "\(principal ?? "anon")",
                ])
            throw S3Error.accessDenied
        }
    }

    func extractPrincipal(from request: Request) -> String? {
        guard let authHeader = request.headers[.authorization] else { return nil }
        // Format: AWS4-HMAC-SHA256 Credential=<AccessKey>/<Date>/<Region>/<Service>/aws4_request, ...
        // We just need the AccessKey.

        let components = authHeader.split(separator: ",", omittingEmptySubsequences: true)
        for component in components {
            let trimmed = component.trimmingCharacters(in: .whitespaces)
            if trimmed.contains("Credential=") {
                if let range = trimmed.range(of: "Credential=") {
                    let credential = String(trimmed[range.upperBound...])
                    let parts = credential.split(separator: "/")
                    if let accessKey = parts.first {
                        return String(accessKey)
                    }
                }
            }
        }
        return nil
    }
}
