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
    static func accessControlPolicy(policy: AccessControlPolicy) -> String {
        return XMLBuilder(
            root: "AccessControlPolicy",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            xml += XMLBuilder.element("Owner") {
                var ownerXML = ""
                ownerXML += XMLBuilder.element("ID", policy.owner.id)
                if let displayName = policy.owner.displayName {
                    ownerXML += XMLBuilder.element("DisplayName", displayName)
                }
                return ownerXML
            }

            xml += XMLBuilder.element("AccessControlList") {
                policy.accessControlList.map { grant in
                    XMLBuilder.element("Grant") {
                        var granteeAttrs: [String: String] = [
                            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance"
                        ]

                        // Map type to xsi:type
                        if grant.grantee.type == "CanonicalUser" {
                            granteeAttrs["xsi:type"] = "CanonicalUser"
                        } else if grant.grantee.type == "Group" {
                            granteeAttrs["xsi:type"] = "Group"
                        } else {
                            granteeAttrs["xsi:type"] = grant.grantee.type  // Fallback
                        }

                        return XMLBuilder.element("Grantee", attributes: granteeAttrs) {
                            var inner = ""
                            if let id = grant.grantee.id {
                                inner += XMLBuilder.element("ID", id)
                            }
                            if let name = grant.grantee.displayName {
                                inner += XMLBuilder.element("DisplayName", name)
                            }
                            if let uri = grant.grantee.uri {
                                inner += XMLBuilder.element("URI", uri)
                            }
                            return inner
                        }
                            + XMLBuilder.element("Permission", grant.permission.rawValue)
                    }
                }.joined()
            }
            return xml
        }.content
    }
    static func versioningConfiguration(config: VersioningConfiguration?) -> String {
        return XMLBuilder(
            root: "VersioningConfiguration",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            if let config = config {
                return XMLBuilder.element("Status", config.status.rawValue)
            } else {
                return XMLBuilder.element(
                    "Status", VersioningConfiguration.Status.suspended.rawValue)
            }
        }.content
    }
    static func listVersionsResult(
        bucket: String, result: ListVersionsResult, prefix: String?, delimiter: String?,
        keyMarker: String?,
        versionIdMarker: String?, maxKeys: Int?
    ) -> String {
        return XMLBuilder(
            root: "ListVersionsResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            xml += XMLBuilder.element("Name", bucket)
            xml += XMLBuilder.element("Prefix", prefix ?? "")
            xml += XMLBuilder.element("KeyMarker", keyMarker ?? "")
            xml += XMLBuilder.element("VersionIdMarker", versionIdMarker ?? "")
            xml += XMLBuilder.element("MaxKeys", String(maxKeys ?? 1000))
            if let delimiter = delimiter {
                xml += XMLBuilder.element("Delimiter", delimiter)
            }
            xml += XMLBuilder.element("IsTruncated", String(result.isTruncated))

            if let nextKeyMarker = result.nextKeyMarker {
                xml += XMLBuilder.element("NextKeyMarker", nextKeyMarker)
            }
            if let nextVersionIdMarker = result.nextVersionIdMarker {
                xml += XMLBuilder.element("NextVersionIdMarker", nextVersionIdMarker)
            }

            xml += result.versions.map { version in
                let lastModified = ISO8601DateFormatter().string(from: version.lastModified)
                let ownerXML = XMLBuilder.element("Owner") {
                    XMLBuilder.element("ID", version.owner ?? "")
                        + XMLBuilder.element("DisplayName", version.owner ?? "")
                }

                if version.isDeleteMarker {
                    return XMLBuilder.element("DeleteMarker") {
                        XMLBuilder.element("Key", version.key)
                            + XMLBuilder.element("VersionId", version.versionId)
                            + XMLBuilder.element("IsLatest", String(version.isLatest))
                            + XMLBuilder.element("LastModified", lastModified)
                            + ownerXML
                    }
                } else {
                    return XMLBuilder.element("Version") {
                        XMLBuilder.element("Key", version.key)
                            + XMLBuilder.element("VersionId", version.versionId)
                            + XMLBuilder.element("IsLatest", String(version.isLatest))
                            + XMLBuilder.element("LastModified", lastModified)
                            + XMLBuilder.element("ETag", "\"\(version.eTag ?? "")\"")
                            + XMLBuilder.element("Size", String(version.size))
                            + XMLBuilder.element("StorageClass", "STANDARD")
                            + ownerXML
                    }
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

    static func taggingConfiguration(tags: [S3Tag]) -> String {
        return XMLBuilder(
            root: "Tagging",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            XMLBuilder.element("TagSet") {
                tags.map { tag in
                    XMLBuilder.element("Tag") {
                        XMLBuilder.element("Key", tag.key) + XMLBuilder.element("Value", tag.value)
                    }
                }.joined()
            }
        }.content
    }

    static func parseTagging(xml: String) -> [S3Tag] {
        var tags: [S3Tag] = []
        let tagPattern = "<Tag>(.*?)</Tag>"
        let keyPattern = "<Key>(.*?)</Key>"
        let valuePattern = "<Value>(.*?)</Value>"

        let tagRegex = try! NSRegularExpression(
            pattern: tagPattern, options: [.dotMatchesLineSeparators])
        let keyRegex = try! NSRegularExpression(pattern: keyPattern, options: [])
        let valueRegex = try! NSRegularExpression(pattern: valuePattern, options: [])

        let nsString = xml as NSString
        let matches = tagRegex.matches(
            in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in matches {
            let tagContent = nsString.substring(with: match.range(at: 1))
            let tagNsString = tagContent as NSString

            var key = ""
            var value = ""

            if let keyMatch = keyRegex.firstMatch(
                in: tagContent, options: [],
                range: NSRange(location: 0, length: tagNsString.length))
            {
                key = tagNsString.substring(with: keyMatch.range(at: 1))
            }

            if let valueMatch = valueRegex.firstMatch(
                in: tagContent, options: [],
                range: NSRange(location: 0, length: tagNsString.length))
            {
                value = tagNsString.substring(with: valueMatch.range(at: 1))
            }

            if !key.isEmpty {
                tags.append(S3Tag(key: key, value: value))
            }
        }

        return tags
    }

    // MARK: - Lifecycle

    static func lifecycleConfiguration(config: LifecycleConfiguration) -> String {
        return XMLBuilder(
            root: "LifecycleConfiguration",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            config.rules.map { rule in
                XMLBuilder.element("Rule") {
                    var ruleXML = ""
                    if let id = rule.id {
                        ruleXML += XMLBuilder.element("ID", id)
                    }
                    ruleXML += XMLBuilder.element("Filter") {
                        XMLBuilder.element("Prefix", rule.filter.prefix ?? "")
                    }
                    ruleXML += XMLBuilder.element("Status", rule.status.rawValue)
                    if let expiration = rule.expiration {
                        ruleXML += XMLBuilder.element("Expiration") {
                            var expXML = ""
                            if let days = expiration.days {
                                expXML += XMLBuilder.element("Days", String(days))
                            }
                            if let date = expiration.date {
                                let dateString = ISO8601DateFormatter().string(from: date)
                                expXML += XMLBuilder.element("Date", dateString)
                            }
                            if let deleteMarker = expiration.expiredObjectDeleteMarker {
                                expXML += XMLBuilder.element(
                                    "ExpiredObjectDeleteMarker", String(deleteMarker))
                            }
                            return expXML
                        }
                    }
                    if let noncurrentExpiration = rule.noncurrentVersionExpiration {
                        ruleXML += XMLBuilder.element("NoncurrentVersionExpiration") {
                            var noncurrentXML = ""
                            if let noncurrentDays = noncurrentExpiration.noncurrentDays {
                                noncurrentXML += XMLBuilder.element("NoncurrentDays", String(noncurrentDays))
                            }
                            if let newerVersions = noncurrentExpiration.newerNoncurrentVersions {
                                noncurrentXML += XMLBuilder.element("NewerNoncurrentVersions", String(newerVersions))
                            }
                            return noncurrentXML
                        }
                    }
                    return ruleXML
                }
            }.joined()
        }.content
    }

    static func parseLifecycle(xml: String) -> LifecycleConfiguration {
        var rules: [LifecycleConfiguration.Rule] = []

        let rulePattern = "<Rule>(.*?)</Rule>"
        let idPattern = "<ID>(.*?)</ID>"
        let statusPattern = "<Status>(.*?)</Status>"
        let prefixPattern = "<Prefix>(.*?)</Prefix>"
        let expirationPattern = "<Expiration>(.*?)</Expiration>"
        let daysPattern = "<Days>(\\d+)</Days>"
        let noncurrentExpirationPattern = "<NoncurrentVersionExpiration>(.*?)</NoncurrentVersionExpiration>"
        let noncurrentDaysPattern = "<NoncurrentDays>(\\d+)</NoncurrentDays>"
        let newerNoncurrentVersionsPattern = "<NewerNoncurrentVersions>(\\d+)</NewerNoncurrentVersions>"

        let ruleRegex = try! NSRegularExpression(
            pattern: rulePattern, options: [.dotMatchesLineSeparators])
        let idRegex = try! NSRegularExpression(pattern: idPattern, options: [])
        let statusRegex = try! NSRegularExpression(pattern: statusPattern, options: [])
        let prefixRegex = try! NSRegularExpression(pattern: prefixPattern, options: [])
        let expirationRegex = try! NSRegularExpression(
            pattern: expirationPattern, options: [.dotMatchesLineSeparators])
        let daysRegex = try! NSRegularExpression(pattern: daysPattern, options: [])
        let noncurrentExpirationRegex = try! NSRegularExpression(
            pattern: noncurrentExpirationPattern, options: [.dotMatchesLineSeparators])
        let noncurrentDaysRegex = try! NSRegularExpression(pattern: noncurrentDaysPattern, options: [])
        let newerNoncurrentVersionsRegex = try! NSRegularExpression(pattern: newerNoncurrentVersionsPattern, options: [])

        let nsString = xml as NSString
        let matches = ruleRegex.matches(
            in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in matches {
            let ruleContent = nsString.substring(with: match.range(at: 1))
            let ruleNsString = ruleContent as NSString

            var id: String? = nil
            if let idMatch = idRegex.firstMatch(
                in: ruleContent, options: [],
                range: NSRange(location: 0, length: ruleNsString.length))
            {
                id = ruleNsString.substring(with: idMatch.range(at: 1))
            }

            var status: LifecycleConfiguration.Rule.Status = .enabled
            if let statusMatch = statusRegex.firstMatch(
                in: ruleContent, options: [],
                range: NSRange(location: 0, length: ruleNsString.length)),
                let s = LifecycleConfiguration.Rule.Status(
                    rawValue: ruleNsString.substring(with: statusMatch.range(at: 1)))
            {
                status = s
            }

            var prefix: String? = nil
            if let prefixMatch = prefixRegex.firstMatch(
                in: ruleContent, options: [],
                range: NSRange(location: 0, length: ruleNsString.length))
            {
                prefix = ruleNsString.substring(with: prefixMatch.range(at: 1))
            }

            var expiration: LifecycleConfiguration.Rule.Expiration? = nil
            if let expMatch = expirationRegex.firstMatch(
                in: ruleContent, options: [],
                range: NSRange(location: 0, length: ruleNsString.length))
            {
                let expContent = ruleNsString.substring(with: expMatch.range(at: 1))
                let expNsString = expContent as NSString

                var days: Int? = nil
                if let daysMatch = daysRegex.firstMatch(
                    in: expContent, options: [],
                    range: NSRange(location: 0, length: expNsString.length))
                {
                    days = Int(expNsString.substring(with: daysMatch.range(at: 1)))
                }
                expiration = LifecycleConfiguration.Rule.Expiration(days: days)
            }

            var noncurrentVersionExpiration: LifecycleConfiguration.Rule.NoncurrentVersionExpiration? = nil
            if let noncurrentMatch = noncurrentExpirationRegex.firstMatch(
                in: ruleContent, options: [],
                range: NSRange(location: 0, length: ruleNsString.length))
            {
                let noncurrentContent = ruleNsString.substring(with: noncurrentMatch.range(at: 1))
                let noncurrentNsString = noncurrentContent as NSString

                var noncurrentDays: Int? = nil
                if let noncurrentDaysMatch = noncurrentDaysRegex.firstMatch(
                    in: noncurrentContent, options: [],
                    range: NSRange(location: 0, length: noncurrentNsString.length))
                {
                    noncurrentDays = Int(noncurrentNsString.substring(with: noncurrentDaysMatch.range(at: 1)))
                }

                var newerNoncurrentVersions: Int? = nil
                if let newerMatch = newerNoncurrentVersionsRegex.firstMatch(
                    in: noncurrentContent, options: [],
                    range: NSRange(location: 0, length: noncurrentNsString.length))
                {
                    newerNoncurrentVersions = Int(noncurrentNsString.substring(with: newerMatch.range(at: 1)))
                }

                if noncurrentDays != nil || newerNoncurrentVersions != nil {
                    noncurrentVersionExpiration = LifecycleConfiguration.Rule.NoncurrentVersionExpiration(
                        noncurrentDays: noncurrentDays, newerNoncurrentVersions: newerNoncurrentVersions)
                }
            }

            rules.append(
                LifecycleConfiguration.Rule(
                    id: id,
                    status: status,
                    filter: LifecycleConfiguration.Rule.Filter(prefix: prefix),
                    expiration: expiration,
                    noncurrentVersionExpiration: noncurrentVersionExpiration
                ))
        }

        return LifecycleConfiguration(rules: rules)
    }
}

// Helper to expose private content property from XMLBuilder because I defined it private but need it here.
// Actually, I defined it private in XMLBuilder struct, so I should expose a public property or method.
// I will assume for now I can edit XMLBuilder if needed, but wait, I defined 'content' as private var, but init populates it.
// I need to add a public getter for 'content' in XMLBuilder.
// Let me quickly fix XMLBuilder first to be safe.
