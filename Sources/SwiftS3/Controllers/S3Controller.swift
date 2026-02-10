import Foundation
import HTTPTypes
import Hummingbird
import NIO

struct S3Controller {
    let storage: any StorageBackend

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
            ":bucket/**",
            use: { request, context in
                try await self.postObject(request: request, context: context)
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
        try await storage.createBucket(name: bucket)
        return Response(status: .ok)
    }

    @Sendable func deleteBucket(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")
        try await storage.deleteBucket(name: bucket)
        return Response(status: .noContent)
    }

    @Sendable func headBucket(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")
        try await storage.headBucket(name: bucket)
        return Response(status: .ok)
    }

    @Sendable func listObjects(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let bucket = try context.parameters.require("bucket")
        let query = parseQuery(request.uri.query)
        let prefix = query["prefix"]
        let delimiter = query["delimiter"]
        let marker = query["marker"]
        let listType = query["list-type"]
        let continuationToken = query["continuation-token"]
        let maxKeys = query["max-keys"].flatMap { Int($0) }

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

    @Sendable func putObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)

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

        return Response(status: .ok, headers: [.eTag: etag])
    }

    @Sendable func postObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)
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

            let uploadId = try await storage.createMultipartUpload(
                bucket: bucket, key: key, metadata: metadata)
            let xml = XML.initiateMultipartUploadResult(
                bucket: bucket, key: key, uploadId: uploadId)
            return Response(
                status: .ok, headers: [.contentType: "application/xml"],
                body: .init(byteBuffer: ByteBuffer(string: xml)))
        } else if let uploadId = query["uploadId"] {
            // Complete Multipart Upload
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
        }

        return Response(status: .badRequest)
    }

    @Sendable func getObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)

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
        let query = parseQuery(request.uri.query)

        if let uploadId = query["uploadId"] {
            try await storage.abortMultipartUpload(bucket: bucket, key: key, uploadId: uploadId)
            return Response(status: .noContent)
        }

        try await storage.deleteObject(bucket: bucket, key: key)
        return Response(status: .noContent)
    }

    @Sendable func headObject(request: Request, context: some RequestContext) async throws
        -> Response
    {
        let (bucket, key) = try parsePath(request.uri.path)

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

}
