import Hummingbird
import Foundation
import NIO

struct S3Controller {
    let storage: any StorageBackend
    
    func addRoutes<Context: RequestContext>(to router: some Router<Context>) {
        // List Buckets (Service)
        router.get("/", use: { request, context in
            try await self.listBuckets(request: request, context: context)
        })
        
        // Bucket Operations
        router.put(":bucket", use: { request, context in
            try await self.createBucket(request: request, context: context)
        })
        router.delete(":bucket", use: { request, context in
            try await self.deleteBucket(request: request, context: context)
        })
        router.get(":bucket", use: { request, context in
            try await self.listObjects(request: request, context: context)
        })
        
        // Object Operations
        // Recursive wildcard for key
        router.put(":bucket/**", use: { request, context in
            try await self.putObject(request: request, context: context)
        })
        router.get(":bucket/**", use: { request, context in
            try await self.getObject(request: request, context: context)
        })
        router.delete(":bucket/**", use: { request, context in
            try await self.deleteObject(request: request, context: context)
        })
        router.head(":bucket/**", use: { request, context in
            try await self.headObject(request: request, context: context)
        })
    }
    
    @Sendable func listBuckets(request: Request, context: some RequestContext) async throws -> Response {
        let buckets = try await storage.listBuckets()
        let xml = XML.listBuckets(buckets: buckets)
        let headers: HTTPFields = [.contentType: "application/xml"]
        return Response(status: .ok, headers: headers, body: .init(byteBuffer: ByteBuffer(string: xml)))
    }
    
    @Sendable func createBucket(request: Request, context: some RequestContext) async throws -> Response {
        let bucket = try context.parameters.require("bucket")
        try await storage.createBucket(name: bucket)
        return Response(status: .ok)
    }
    
    @Sendable func deleteBucket(request: Request, context: some RequestContext) async throws -> Response {
        let bucket = try context.parameters.require("bucket")
        try await storage.deleteBucket(name: bucket)
        return Response(status: .noContent)
    }
    
    @Sendable func listObjects(request: Request, context: some RequestContext) async throws -> Response {
        let bucket = try context.parameters.require("bucket")
        let objects = try await storage.listObjects(bucket: bucket)
        let xml = XML.listObjects(bucket: bucket, objects: objects)
        let headers: HTTPFields = [.contentType: "application/xml"]
        return Response(status: .ok, headers: headers, body: .init(byteBuffer: ByteBuffer(string: xml)))
    }
    
    @Sendable func putObject(request: Request, context: some RequestContext) async throws -> Response {
        let (bucket, key) = try parsePath(request.uri.path)
        
        let contentLength = request.headers[.contentLength].flatMap { Int64($0) }
        
        // Stream body
        let etag = try await storage.putObject(bucket: bucket, key: key, data: request.body, size: contentLength)
        
        return Response(status: .ok, headers: [.eTag: etag])
    }
    
    @Sendable func getObject(request: Request, context: some RequestContext) async throws -> Response {
        let (bucket, key) = try parsePath(request.uri.path)
        
        let (metadata, body) = try await storage.getObject(bucket: bucket, key: key)
        
        var headers: HTTPFields = [
            .lastModified: ISO8601DateFormatter().string(from: metadata.lastModified),
            .contentLength: String(metadata.size),
            .contentType: "application/octet-stream"
        ]
        
        if let etag = metadata.eTag {
            headers[.eTag] = etag
        }

        if let body = body {
             return Response(status: .ok, headers: headers, body: .init(asyncSequence: body))
        } else {
             return Response(status: .ok, headers: headers, body: .init(byteBuffer: ByteBuffer()))
        }
    }
    
    @Sendable func deleteObject(request: Request, context: some RequestContext) async throws -> Response {
        let (bucket, key) = try parsePath(request.uri.path)
        try await storage.deleteObject(bucket: bucket, key: key)
        return Response(status: .noContent)
    }
    
     @Sendable func headObject(request: Request, context: some RequestContext) async throws -> Response {
        let (bucket, key) = try parsePath(request.uri.path)
        
        let metadata = try await storage.getObjectMetadata(bucket: bucket, key: key)
        
        var headers: HTTPFields = [
            .lastModified: ISO8601DateFormatter().string(from: metadata.lastModified),
            .contentLength: String(metadata.size)
        ]
        if let etag = metadata.eTag {
            headers[.eTag] = etag
        }
        
        return Response(status: .ok, headers: headers)
    }

    private func parsePath(_ path: String) throws -> (String, String) {
        let components = path.split(separator: "/")
        guard components.count >= 2 else {
            throw S3Error.noSuchKey // Or bad request
        }
        let bucket = String(components[0])
        let key = components.dropFirst().joined(separator: "/")
        return (bucket, key)
    }
    
}
