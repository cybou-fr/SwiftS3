import Foundation

struct XML {
    static func listBuckets(buckets: [(name: String, created: Date)]) -> String {
        return XMLBuilder(root: "ListAllMyBucketsResult") {
            XMLBuilder.element("Buckets") {
                buckets.map { bucket in
                    XMLBuilder.element("Bucket") {
                        XMLBuilder.element("Name", bucket.name)
                            + XMLBuilder.element(
                                "CreationDate", ISO8601DateFormatter().string(from: bucket.created))
                    }
                }.joined()
            }
        }.content
    }

    static func listObjects(
        bucket: String, result: ListObjectsResult, prefix: String, marker: String, maxKeys: Int,
        isTruncated: Bool
    ) -> String {
        return XMLBuilder(
            root: "ListBucketResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            xml += XMLBuilder.element("Name", bucket)
            xml += XMLBuilder.element("Prefix", prefix)
            xml += XMLBuilder.element("Marker", marker)
            xml += XMLBuilder.element("MaxKeys", String(maxKeys))
            xml += XMLBuilder.element("IsTruncated", String(isTruncated))

            if let nextMarker = result.nextMarker {
                xml += XMLBuilder.element("NextMarker", nextMarker)
            }

            xml += result.objects.map { object in
                XMLBuilder.element("Contents") {
                    XMLBuilder.element("Key", object.key)
                        + XMLBuilder.element(
                            "LastModified", ISO8601DateFormatter().string(from: object.lastModified)
                        ) + XMLBuilder.element("ETag", "\"\(object.eTag ?? "")\"")
                        + XMLBuilder.element("Size", String(object.size))
                        + XMLBuilder.element("StorageClass", "STANDARD")
                }
            }.joined()

            xml += result.commonPrefixes.map { prefix in
                XMLBuilder.element("CommonPrefixes") {
                    XMLBuilder.element("Prefix", prefix)
                }
            }.joined()

            return xml
        }.content
    }

    // List Objects V2
    static func listObjectsV2(
        bucket: String, result: ListObjectsResult, prefix: String, continuationToken: String,
        maxKeys: Int,
        isTruncated: Bool, keyCount: Int
    ) -> String {
        return XMLBuilder(
            root: "ListBucketResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            xml += XMLBuilder.element("Name", bucket)
            xml += XMLBuilder.element("Prefix", prefix)
            xml += XMLBuilder.element("MaxKeys", String(maxKeys))
            xml += XMLBuilder.element("KeyCount", String(keyCount))
            xml += XMLBuilder.element("IsTruncated", String(isTruncated))

            if !continuationToken.isEmpty {
                xml += XMLBuilder.element("ContinuationToken", continuationToken)
            }

            if let nextContinuationToken = result.nextContinuationToken {
                xml += XMLBuilder.element("NextContinuationToken", nextContinuationToken)
            }

            xml += result.objects.map { object in
                XMLBuilder.element("Contents") {
                    XMLBuilder.element("Key", object.key)
                        + XMLBuilder.element(
                            "LastModified", ISO8601DateFormatter().string(from: object.lastModified)
                        ) + XMLBuilder.element("ETag", "\"\(object.eTag ?? "")\"")
                        + XMLBuilder.element("Size", String(object.size))
                        + XMLBuilder.element("StorageClass", "STANDARD")
                }
            }.joined()

            xml += result.commonPrefixes.map { prefix in
                XMLBuilder.element("CommonPrefixes") {
                    XMLBuilder.element("Prefix", prefix)
                }
            }.joined()

            return xml
        }.content
    }

    static func copyObjectResult(metadata: ObjectMetadata) -> String {
        let lastModified = ISO8601DateFormatter().string(from: metadata.lastModified)
        let etag = metadata.eTag ?? ""
        return XMLBuilder(
            root: "CopyObjectResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            XMLBuilder.element("LastModified", lastModified) + XMLBuilder.element("ETag", etag)
        }.content
    }

    static func initiateMultipartUploadResult(bucket: String, key: String, uploadId: String)
        -> String
    {
        return XMLBuilder(
            root: "InitiateMultipartUploadResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            XMLBuilder.element("Bucket", bucket) + XMLBuilder.element("Key", key)
                + XMLBuilder.element("UploadId", uploadId)
        }.content
    }

    static func completeMultipartUploadResult(
        bucket: String, key: String, eTag: String, location: String
    ) -> String {
        return XMLBuilder(
            root: "CompleteMultipartUploadResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            XMLBuilder.element("Location", location) + XMLBuilder.element("Bucket", bucket)
                + XMLBuilder.element("Key", key) + XMLBuilder.element("ETag", "\"\(eTag)\"")
        }.content
    }

    static func parseCompleteMultipartUpload(xml: String) -> [PartInfo] {
        // Simple parsing using regex to avoid dependencies
        // Assuming format: <Part><PartNumber>1</PartNumber><ETag>"..."</ETag></Part>
        var parts: [PartInfo] = []

        let partPattern = "<Part>(.*?)</Part>"
        let numberPattern = "<PartNumber>(\\d+)</PartNumber>"
        let currentEtagPattern = "<ETag>\"?([^\"]+)\"?</ETag>"  // Handles quotes if present

        let partRegex = try! NSRegularExpression(
            pattern: partPattern, options: [.dotMatchesLineSeparators])
        let numberRegex = try! NSRegularExpression(pattern: numberPattern, options: [])
        let etagRegex = try! NSRegularExpression(pattern: currentEtagPattern, options: [])

        let nsString = xml as NSString
        let matches = partRegex.matches(
            in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in matches {
            let partContent = nsString.substring(with: match.range(at: 1))
            let partNsString = partContent as NSString

            var partNumber = 0
            var eTag = ""

            if let numMatch = numberRegex.firstMatch(
                in: partContent, options: [],
                range: NSRange(location: 0, length: partNsString.length))
            {
                partNumber = Int(partNsString.substring(with: numMatch.range(at: 1))) ?? 0
            }

            if let etagMatch = etagRegex.firstMatch(
                in: partContent, options: [],
                range: NSRange(location: 0, length: partNsString.length))
            {
                eTag = partNsString.substring(with: etagMatch.range(at: 1))
            }

            if partNumber > 0 && !eTag.isEmpty {
                parts.append(PartInfo(partNumber: partNumber, eTag: eTag))
            }
        }

        return parts.sorted { $0.partNumber < $1.partNumber }
    }

    static func deleteResult(
        deleted: [String], errors: [(key: String, code: String, message: String)]
    ) -> String {
        return XMLBuilder(
            root: "DeleteResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            xml += deleted.map { key in
                XMLBuilder.element("Deleted") {
                    XMLBuilder.element("Key", key)
                }
            }.joined()

            xml += errors.map { error in
                XMLBuilder.element("Error") {
                    XMLBuilder.element("Key", error.key) + XMLBuilder.element("Code", error.code)
                        + XMLBuilder.element("Message", error.message)
                }
            }.joined()

            return xml
        }.content
    }

    // Helper to parse DeleteObjects request body
    static func parseDeleteObjects(xml: String) -> [String] {
        var keys: [String] = []
        // Simple regex to find <Key>...</Key> inside <Delete>...<Object>...</Object>...</Delete>
        // But the input XML structure is:
        // <Delete>
        //   <Object><Key>key1</Key></Object>
        //   <Object><Key>key2</Key></Object>
        // </Delete>

        let keyPattern = "<Key>(.*?)</Key>"
        let keyRegex = try! NSRegularExpression(
            pattern: keyPattern, options: [.dotMatchesLineSeparators])
        let nsString = xml as NSString
        let matches = keyRegex.matches(
            in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in matches {
            let key = nsString.substring(with: match.range(at: 1))
            keys.append(key)
        }
        return keys
    }
}

// Helper to expose private content property from XMLBuilder because I defined it private but need it here.
// Actually, I defined it private in XMLBuilder struct, so I should expose a public property or method.
// I will assume for now I can edit XMLBuilder if needed, but wait, I defined 'content' as private var, but init populates it.
// I need to add a public getter for 'content' in XMLBuilder.
// Let me quickly fix XMLBuilder first to be safe.
