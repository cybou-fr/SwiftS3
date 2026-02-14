import Foundation

/// Represents an object to be deleted in a bulk delete operation
struct DeleteObject: Equatable {
    let key: String
    let versionId: String?
}

/// Utility struct for generating XML responses compatible with AWS S3 API.
/// Provides static methods for creating XML documents for various S3 operations.
/// All methods return properly formatted XML strings that match AWS S3 response formats.
struct XML {
    /// Generates XML for listing all buckets owned by a user.
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

    /// Generates XML response for listing objects in a bucket.
    /// Supports AWS S3 ListObjectsV1 API format with pagination support.
    ///
    /// - Parameters:
    ///   - bucket: The bucket name being listed
    ///   - result: Query result containing objects and pagination info
    ///   - prefix: Object key prefix filter (empty string for no filter)
    ///   - marker: Pagination marker for continuing listings
    ///   - maxKeys: Maximum number of objects to return (0-1000)
    ///   - isTruncated: Whether the listing is truncated due to maxKeys limit
    /// - Returns: XML string in AWS S3 ListBucketResult format
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

    /// Generates XML response for list objects V2 API calls
    /// - Parameters:
    ///   - bucket: Name of the bucket
    ///   - result: ListObjectsResult containing objects and pagination info
    ///   - prefix: Key prefix filter
    ///   - continuationToken: Token for pagination
    ///   - maxKeys: Maximum number of keys to return
    ///   - isTruncated: Whether the result is truncated
    ///   - keyCount: Number of keys returned in this response
    /// - Returns: XML string formatted according to S3 API specification for list objects V2 responses
    /// - Note: Includes object details, common prefixes, and pagination information
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

    /// Generates XML for copy object operation result.
    /// Returns the metadata of the newly created copy in AWS S3 format.
    ///
    /// - Parameter metadata: Metadata of the copied object
    /// - Returns: XML string in CopyObjectResult format
    static func copyObjectResult(metadata: ObjectMetadata) -> String {
        let lastModified = ISO8601DateFormatter().string(from: metadata.lastModified)
        let etag = metadata.eTag ?? ""
        return XMLBuilder(
            root: "CopyObjectResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            XMLBuilder.element("LastModified", lastModified) + XMLBuilder.element("ETag", "\"\(etag)\"")
        }.content
    }

    /// Generates XML for multipart upload initiation result.
    /// Returns the upload ID and bucket/key information for the initiated upload.
    ///
    /// - Parameters:
    ///   - bucket: Target bucket name
    ///   - key: Object key
    ///   - uploadId: Unique multipart upload identifier
    /// - Returns: XML string in InitiateMultipartUploadResult format
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

    /// Generates XML for completed multipart upload result.
    /// Returns the final object information after successful multipart upload completion.
    ///
    /// - Parameters:
    ///   - bucket: Target bucket name
    ///   - key: Object key
    ///   - eTag: Combined ETag of all parts
    ///   - location: Object location URL
    /// - Returns: XML string in CompleteMultipartUploadResult format
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

    /// Parses XML containing multipart upload completion instructions.
    /// Extracts part numbers and ETags from the CompleteMultipartUpload request body.
    ///
    /// - Parameter xml: XML string containing part information
    /// - Returns: Array of PartInfo structs with part numbers and ETags
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

        return parts
    }

    /// Generates XML response for bulk delete operations
    /// - Parameters:
    ///   - deleted: Array of successfully deleted object keys
    ///   - errors: Array of tuples containing error details (key, code, message) for failed deletions
    /// - Returns: XML string formatted according to S3 API specification for delete results
    static func deleteResult(
        deleted: [(key: String, versionId: String?, isDeleteMarker: Bool, deleteMarkerVersionId: String?)],
        errors: [(key: String, code: String, message: String)]
    ) -> String {
        return XMLBuilder(
            root: "DeleteResult",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            xml += deleted.map { (key, versionId, isDeleteMarker, deleteMarkerVersionId) in
                XMLBuilder.element("Deleted") {
                    var content = XMLBuilder.element("Key", key)
                    if let versionId = versionId {
                        content += XMLBuilder.element("VersionId", versionId)
                    }
                    if isDeleteMarker {
                        content += XMLBuilder.element("DeleteMarker", "true")
                        if let deleteMarkerVersionId = deleteMarkerVersionId {
                            content += XMLBuilder.element("DeleteMarkerVersionId", deleteMarkerVersionId)
                        }
                    }
                    return content
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

    /// Parses XML input for bulk delete operations to extract object keys and version IDs
    /// - Parameter xml: XML string containing delete request with object keys and optional version IDs
    /// - Returns: Array of DeleteObject instances containing key and optional version ID
    /// - Note: Uses regex to extract keys and version IDs from <Object> elements within <Delete> elements
    // Helper to parse DeleteObjects request body
    static func parseDeleteObjects(xml: String) -> [DeleteObject] {
        var objects: [DeleteObject] = []
        // Parse <Object> elements which may contain <Key> and optional <VersionId>
        // Structure:
        // <Delete>
        //   <Object><Key>key1</Key><VersionId>version1</VersionId></Object>
        //   <Object><Key>key2</Key></Object>
        // </Delete>

        let objectPattern = "<Object>(.*?)</Object>"
        let objectRegex = try! NSRegularExpression(
            pattern: objectPattern, options: [.dotMatchesLineSeparators, .caseInsensitive])
        let nsString = xml as NSString
        let objectMatches = objectRegex.matches(
            in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for objectMatch in objectMatches {
            let objectContent = nsString.substring(with: objectMatch.range(at: 1))

            // Extract key
            let keyPattern = "<Key>(.*?)</Key>"
            let keyRegex = try! NSRegularExpression(
                pattern: keyPattern, options: [.dotMatchesLineSeparators, .caseInsensitive])
            let keyMatches = keyRegex.matches(
                in: objectContent, options: [], range: NSRange(location: 0, length: (objectContent as NSString).length))

            guard let keyMatch = keyMatches.first else { continue }
            let key = (objectContent as NSString).substring(with: keyMatch.range(at: 1))

            // Extract optional version ID
            var versionId: String? = nil
            let versionPattern = "<VersionId>(.*?)</VersionId>"
            let versionRegex = try! NSRegularExpression(
                pattern: versionPattern, options: [.dotMatchesLineSeparators, .caseInsensitive])
            let versionMatches = versionRegex.matches(
                in: objectContent, options: [], range: NSRange(location: 0, length: (objectContent as NSString).length))

            if let versionMatch = versionMatches.first {
                versionId = (objectContent as NSString).substring(with: versionMatch.range(at: 1))
            }

            objects.append(DeleteObject(key: key, versionId: versionId))
        }
        return objects
    }

    /// Generates XML representation of an access control policy for S3 objects/buckets
    /// - Parameter policy: The access control policy containing owner and grant information
    /// - Returns: XML string formatted according to S3 API specification for ACL responses
    /// - Note: Includes owner information and access control list with grantee details and permissions
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

    /// Parses XML Access Control Policy to extract owner and grants
    /// - Parameter xml: XML string containing access control policy
    /// - Returns: AccessControlPolicy object with parsed owner and grants
    static func parseAccessControlPolicy(xml: String) -> AccessControlPolicy {
        var owner: Owner?
        var grants: [Grant] = []

        // Parse owner
        let ownerPattern = "<Owner>(.*?)</Owner>"
        let ownerRegex = try! NSRegularExpression(pattern: ownerPattern, options: [.dotMatchesLineSeparators])
        let ownerIdPattern = "<ID>(.*?)</ID>"
        let ownerDisplayNamePattern = "<DisplayName>(.*?)</DisplayName>"
        let ownerIdRegex = try! NSRegularExpression(pattern: ownerIdPattern, options: [])
        let ownerDisplayNameRegex = try! NSRegularExpression(pattern: ownerDisplayNamePattern, options: [])

        let nsString = xml as NSString
        if let ownerMatch = ownerRegex.firstMatch(in: xml, options: [], range: NSRange(location: 0, length: nsString.length)) {
            let ownerContent = nsString.substring(with: ownerMatch.range(at: 1))
            let ownerNsString = ownerContent as NSString

            var ownerId: String?
            var ownerDisplayName: String?

            if let idMatch = ownerIdRegex.firstMatch(in: ownerContent, options: [], range: NSRange(location: 0, length: ownerNsString.length)) {
                ownerId = ownerNsString.substring(with: idMatch.range(at: 1))
            }

            if let nameMatch = ownerDisplayNameRegex.firstMatch(in: ownerContent, options: [], range: NSRange(location: 0, length: ownerNsString.length)) {
                ownerDisplayName = ownerNsString.substring(with: nameMatch.range(at: 1))
            }

            if let ownerId = ownerId {
                owner = Owner(id: ownerId, displayName: ownerDisplayName)
            }
        }

        // Parse grants
        let grantPattern = "<Grant>(.*?)</Grant>"
        let grantRegex = try! NSRegularExpression(pattern: grantPattern, options: [.dotMatchesLineSeparators])
        let granteePattern = "<Grantee[^>]*>(.*?)</Grantee>"
        let granteeRegex = try! NSRegularExpression(pattern: granteePattern, options: [.dotMatchesLineSeparators])
        let permissionPattern = "<Permission>(.*?)</Permission>"
        let permissionRegex = try! NSRegularExpression(pattern: permissionPattern, options: [])

        let grantMatches = grantRegex.matches(in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for grantMatch in grantMatches {
            let grantContent = nsString.substring(with: grantMatch.range(at: 1))
            let grantNsString = grantContent as NSString

            // Parse grantee
            var grantee: Grantee?
            if let granteeMatch = granteeRegex.firstMatch(in: grantContent, options: [], range: NSRange(location: 0, length: grantNsString.length)) {
                let granteeContent = grantNsString.substring(with: granteeMatch.range(at: 1))
                let granteeNsString = granteeContent as NSString

                var granteeId: String?
                var granteeDisplayName: String?
                var granteeType = "CanonicalUser"
                var granteeUri: String?

                // Check for xsi:type attribute
                if grantContent.contains("xsi:type=\"Group\"") {
                    granteeType = "Group"
                }

                if let idMatch = ownerIdRegex.firstMatch(in: granteeContent, options: [], range: NSRange(location: 0, length: granteeNsString.length)) {
                    granteeId = granteeNsString.substring(with: idMatch.range(at: 1))
                }

                if let nameMatch = ownerDisplayNameRegex.firstMatch(in: granteeContent, options: [], range: NSRange(location: 0, length: granteeNsString.length)) {
                    granteeDisplayName = granteeNsString.substring(with: nameMatch.range(at: 1))
                }

                let uriPattern = "<URI>(.*?)</URI>"
                let uriRegex = try! NSRegularExpression(pattern: uriPattern, options: [])
                if let uriMatch = uriRegex.firstMatch(in: granteeContent, options: [], range: NSRange(location: 0, length: granteeNsString.length)) {
                    granteeUri = granteeNsString.substring(with: uriMatch.range(at: 1))
                }

                grantee = Grantee(id: granteeId, displayName: granteeDisplayName, type: granteeType, uri: granteeUri)
            }

            // Parse permission
            var permission: Permission?
            if let permissionMatch = permissionRegex.firstMatch(in: grantContent, options: [], range: NSRange(location: 0, length: grantNsString.length)) {
                let permissionStr = grantNsString.substring(with: permissionMatch.range(at: 1))
                permission = Permission(rawValue: permissionStr)
            }

            if let grantee = grantee, let permission = permission {
                grants.append(Grant(grantee: grantee, permission: permission))
            }
        }

        let finalOwner = owner ?? Owner(id: "anonymous")

        return AccessControlPolicy(owner: finalOwner, accessControlList: grants)
    }

    /// Generates XML representation of bucket versioning configuration
    /// - Parameter config: Optional versioning configuration, nil means versioning is suspended
    /// - Returns: XML string formatted according to S3 API specification for versioning responses
    /// - Note: Includes status (Enabled/Suspended) and optional MFA delete setting
    static func versioningConfiguration(config: VersioningConfiguration?) -> String {
        return XMLBuilder(
            root: "VersioningConfiguration",
            attributes: ["xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"]
        ) {
            var xml = ""
            if let config = config {
                xml += XMLBuilder.element("Status", config.status.rawValue)
                if let mfaDelete = config.mfaDelete {
                    xml += XMLBuilder.element("MfaDelete", mfaDelete ? "Enabled" : "Disabled")
                }
            } else {
                xml += XMLBuilder.element("Status", VersioningConfiguration.Status.suspended.rawValue)
            }
            return xml
        }.content
    }

    /// Generates XML response for list object versions API calls
    /// - Parameters:
    ///   - bucket: Name of the bucket
    ///   - result: ListVersionsResult containing versions and pagination info
    ///   - prefix: Optional key prefix filter
    ///   - delimiter: Optional delimiter for grouping keys
    ///   - keyMarker: Optional marker for pagination by key
    ///   - versionIdMarker: Optional marker for pagination by version ID
    ///   - maxKeys: Optional maximum number of results to return
    /// - Returns: XML string formatted according to S3 API specification for list versions responses
    /// - Note: Includes version details, delete markers, and pagination information
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

    /// Generates XML representation of object/bucket tagging configuration
    /// - Parameter tags: Array of S3Tag objects containing key-value pairs
    /// - Returns: XML string formatted according to S3 API specification for tagging responses
    /// - Note: Wraps tags in a TagSet element as required by S3 API
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

    /// Parses XML tagging configuration to extract key-value tag pairs
    /// - Parameter xml: XML string containing tagging configuration
    /// - Returns: Array of S3Tag objects with key-value pairs
    /// - Note: Uses regex to extract tags from <Tag> elements containing <Key> and <Value> elements
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

    /// Generates XML representation of lifecycle configuration for bucket management
    /// - Parameter config: LifecycleConfiguration containing rules for object lifecycle management
    /// - Returns: XML string formatted according to S3 API specification for lifecycle responses
    /// - Note: Includes rules with filters, status, expiration settings, and non-current version handling
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
                        var filterXML = ""
                        if let prefix = rule.filter.prefix {
                            filterXML += XMLBuilder.element("Prefix", prefix)
                        }
                        if let tag = rule.filter.tag {
                            filterXML += XMLBuilder.element("Tag") {
                                XMLBuilder.element("Key", tag.key) + XMLBuilder.element("Value", tag.value)
                            }
                        }
                        return filterXML
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

    /// Parses XML lifecycle configuration to extract lifecycle rules
    /// - Parameter xml: XML string containing lifecycle configuration
    /// - Returns: LifecycleConfiguration object with parsed rules
    /// - Note: Uses regex to extract rules, filters, expiration settings, and non-current version configurations
    static func parseLifecycle(xml: String) -> LifecycleConfiguration {
        var rules: [LifecycleConfiguration.Rule] = []

        let rulePattern = "<Rule>(.*?)</Rule>"
        let idPattern = "<ID>(.*?)</ID>"
        let statusPattern = "<Status>(.*?)</Status>"
        let prefixPattern = "<Prefix>(.*?)</Prefix>"
        let tagPattern = "<Tag>(.*?)</Tag>"
        let tagKeyPattern = "<Key>(.*?)</Key>"
        let tagValuePattern = "<Value>(.*?)</Value>"
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
        let tagRegex = try! NSRegularExpression(pattern: tagPattern, options: [.dotMatchesLineSeparators])
        let tagKeyRegex = try! NSRegularExpression(pattern: tagKeyPattern, options: [])
        let tagValueRegex = try! NSRegularExpression(pattern: tagValuePattern, options: [])
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

            var tag: S3Tag? = nil
            if let tagMatch = tagRegex.firstMatch(
                in: ruleContent, options: [],
                range: NSRange(location: 0, length: ruleNsString.length))
            {
                let tagContent = ruleNsString.substring(with: tagMatch.range(at: 1))
                let tagNsString = tagContent as NSString

                var key: String? = nil
                if let keyMatch = tagKeyRegex.firstMatch(
                    in: tagContent, options: [],
                    range: NSRange(location: 0, length: tagNsString.length))
                {
                    key = tagNsString.substring(with: keyMatch.range(at: 1))
                }

                var value: String? = nil
                if let valueMatch = tagValueRegex.firstMatch(
                    in: tagContent, options: [],
                    range: NSRange(location: 0, length: tagNsString.length))
                {
                    value = tagNsString.substring(with: valueMatch.range(at: 1))
                }

                if let key = key, let value = value {
                    tag = S3Tag(key: key, value: value)
                }
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
                    filter: LifecycleConfiguration.Rule.Filter(prefix: prefix, tag: tag),
                    expiration: expiration,
                    noncurrentVersionExpiration: noncurrentVersionExpiration
                ))
        }

        return LifecycleConfiguration(rules: rules)
    }

    // MARK: - Notification

    /// Generates XML representation of bucket notification configuration
    /// - Parameter config: NotificationConfiguration containing topic, queue, and lambda configurations
    /// - Returns: XML string formatted according to S3 API specification for notification responses
    /// - Note: Includes topic configurations with events, filters, and destination ARNs
    static func notificationConfiguration(config: NotificationConfiguration) -> String {
        return XMLBuilder(root: "NotificationConfiguration") {
            var xml = ""
            
            if let topicConfigs = config.topicConfigurations {
                xml += topicConfigs.map { topicConfig in
                    XMLBuilder.element("TopicConfiguration") {
                        var configXML = ""
                        if let id = topicConfig.id {
                            configXML += XMLBuilder.element("Id", id)
                        }
                        configXML += XMLBuilder.element("Topic", topicConfig.topicArn)
                        configXML += topicConfig.events.map { event in
                            XMLBuilder.element("Event", event.rawValue)
                        }.joined()
                        if let filter = topicConfig.filter {
                            configXML += notificationFilterXML(filter: filter)
                        }
                        return configXML
                    }
                }.joined()
            }
            
            if let queueConfigs = config.queueConfigurations {
                xml += queueConfigs.map { queueConfig in
                    XMLBuilder.element("QueueConfiguration") {
                        var configXML = ""
                        if let id = queueConfig.id {
                            configXML += XMLBuilder.element("Id", id)
                        }
                        configXML += XMLBuilder.element("Queue", queueConfig.queueArn)
                        configXML += queueConfig.events.map { event in
                            XMLBuilder.element("Event", event.rawValue)
                        }.joined()
                        if let filter = queueConfig.filter {
                            configXML += notificationFilterXML(filter: filter)
                        }
                        return configXML
                    }
                }.joined()
            }
            
            if let lambdaConfigs = config.lambdaConfigurations {
                xml += lambdaConfigs.map { lambdaConfig in
                    XMLBuilder.element("CloudFunctionConfiguration") {
                        var configXML = ""
                        if let id = lambdaConfig.id {
                            configXML += XMLBuilder.element("Id", id)
                        }
                        configXML += XMLBuilder.element("CloudFunction", lambdaConfig.lambdaFunctionArn)
                        configXML += lambdaConfig.events.map { event in
                            XMLBuilder.element("Event", event.rawValue)
                        }.joined()
                        if let filter = lambdaConfig.filter {
                            configXML += notificationFilterXML(filter: filter)
                        }
                        return configXML
                    }
                }.joined()
            }
            
            if let webhookConfigs = config.webhookConfigurations {
                xml += webhookConfigs.map { webhookConfig in
                    XMLBuilder.element("WebhookConfiguration") {
                        var configXML = ""
                        if let id = webhookConfig.id {
                            configXML += XMLBuilder.element("Id", id)
                        }
                        configXML += XMLBuilder.element("Url", webhookConfig.url)
                        configXML += webhookConfig.events.map { event in
                            XMLBuilder.element("Event", event.rawValue)
                        }.joined()
                        if let filter = webhookConfig.filter {
                            configXML += notificationFilterXML(filter: filter)
                        }
                        return configXML
                    }
                }.joined()
            }
            
            return xml
        }.content
    }
    
    private static func notificationFilterXML(filter: NotificationFilter) -> String {
        /// Generates XML representation of notification filter configuration
        /// - Parameter filter: NotificationFilter containing key filter rules
        /// - Returns: XML string for filter element with S3Key filter rules
        /// - Note: Only processes key filters, returns empty string if no key filter present
        guard let keyFilter = filter.key else { return "" }
        return XMLBuilder.element("Filter") {
            XMLBuilder.element("S3Key") {
                keyFilter.filterRules.map { rule in
                    XMLBuilder.element("FilterRule") {
                        XMLBuilder.element("Name", rule.name.rawValue) + XMLBuilder.element("Value", rule.value)
                    }
                }.joined()
            }
        }
    }

    /// Parses XML notification configuration
    /// - Parameter xml: XML string containing notification configuration
    /// - Returns: NotificationConfiguration object with parsed configurations
    static func parseNotification(xml: String) -> NotificationConfiguration {
        var topicConfigurations: [TopicConfiguration] = []
        var queueConfigurations: [QueueConfiguration] = []
        var lambdaConfigurations: [LambdaConfiguration] = []

        let nsString = xml as NSString

        // Parse TopicConfigurations
        let topicConfigPattern = "<TopicConfiguration>(.*?)</TopicConfiguration>"
        let topicConfigRegex = try! NSRegularExpression(pattern: topicConfigPattern, options: [.dotMatchesLineSeparators])
        let topicConfigMatches = topicConfigRegex.matches(in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in topicConfigMatches {
            let configContent = nsString.substring(with: match.range(at: 1))
            let configNsString = configContent as NSString

            var id: String?
            var topicArn: String?
            var events: [S3EventType] = []
            var filter: NotificationFilter?

            // Parse ID
            if let idMatch = try! NSRegularExpression(pattern: "<Id>(.*?)</Id>", options: []).firstMatch(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length)) {
                id = configNsString.substring(with: idMatch.range(at: 1))
            }

            // Parse Topic ARN
            if let topicMatch = try! NSRegularExpression(pattern: "<Topic>(.*?)</Topic>", options: []).firstMatch(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length)) {
                topicArn = configNsString.substring(with: topicMatch.range(at: 1))
            }

            // Parse Events
            let eventPattern = "<Event>(.*?)</Event>"
            let eventRegex = try! NSRegularExpression(pattern: eventPattern, options: [])
            let eventMatches = eventRegex.matches(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length))
            for eventMatch in eventMatches {
                let eventStr = configNsString.substring(with: eventMatch.range(at: 1))
                if let event = S3EventType(rawValue: eventStr) {
                    events.append(event)
                }
            }

            // Parse Filter (simplified - would need full implementation)
            if configContent.contains("<Filter>") {
                // For now, skip filter parsing
                filter = nil
            }

            if let topicArn = topicArn {
                topicConfigurations.append(TopicConfiguration(id: id, topicArn: topicArn, events: events, filter: filter))
            }
        }

        // Parse QueueConfigurations (similar pattern)
        let queueConfigPattern = "<QueueConfiguration>(.*?)</QueueConfiguration>"
        let queueConfigRegex = try! NSRegularExpression(pattern: queueConfigPattern, options: [.dotMatchesLineSeparators])
        let queueConfigMatches = queueConfigRegex.matches(in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in queueConfigMatches {
            let configContent = nsString.substring(with: match.range(at: 1))
            let configNsString = configContent as NSString

            var id: String?
            var queueArn: String?
            var events: [S3EventType] = []

            // Parse ID
            if let idMatch = try! NSRegularExpression(pattern: "<Id>(.*?)</Id>", options: []).firstMatch(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length)) {
                id = configNsString.substring(with: idMatch.range(at: 1))
            }

            // Parse Queue ARN
            if let queueMatch = try! NSRegularExpression(pattern: "<Queue>(.*?)</Queue>", options: []).firstMatch(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length)) {
                queueArn = configNsString.substring(with: queueMatch.range(at: 1))
            }

            // Parse Events
            let eventPattern = "<Event>(.*?)</Event>"
            let eventRegex = try! NSRegularExpression(pattern: eventPattern, options: [])
            let eventMatches = eventRegex.matches(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length))
            for eventMatch in eventMatches {
                let eventStr = configNsString.substring(with: eventMatch.range(at: 1))
                if let event = S3EventType(rawValue: eventStr) {
                    events.append(event)
                }
            }

            if let queueArn = queueArn {
                queueConfigurations.append(QueueConfiguration(id: id, queueArn: queueArn, events: events, filter: nil))
            }
        }

        // Parse LambdaConfigurations (similar pattern)
        let lambdaConfigPattern = "<CloudFunctionConfiguration>(.*?)</CloudFunctionConfiguration>"
        let lambdaConfigRegex = try! NSRegularExpression(pattern: lambdaConfigPattern, options: [.dotMatchesLineSeparators])
        let lambdaConfigMatches = lambdaConfigRegex.matches(in: xml, options: [], range: NSRange(location: 0, length: nsString.length))

        for match in lambdaConfigMatches {
            let configContent = nsString.substring(with: match.range(at: 1))
            let configNsString = configContent as NSString

            var id: String?
            var lambdaFunctionArn: String?
            var events: [S3EventType] = []

            // Parse ID
            if let idMatch = try! NSRegularExpression(pattern: "<Id>(.*?)</Id>", options: []).firstMatch(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length)) {
                id = configNsString.substring(with: idMatch.range(at: 1))
            }

            // Parse Lambda ARN
            if let lambdaMatch = try! NSRegularExpression(pattern: "<CloudFunction>(.*?)</CloudFunction>", options: []).firstMatch(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length)) {
                lambdaFunctionArn = configNsString.substring(with: lambdaMatch.range(at: 1))
            }

            // Parse Events
            let eventPattern = "<Event>(.*?)</Event>"
            let eventRegex = try! NSRegularExpression(pattern: eventPattern, options: [])
            let eventMatches = eventRegex.matches(in: configContent, options: [], range: NSRange(location: 0, length: configNsString.length))
            for eventMatch in eventMatches {
                let eventStr = configNsString.substring(with: eventMatch.range(at: 1))
                if let event = S3EventType(rawValue: eventStr) {
                    events.append(event)
                }
            }

            if let lambdaFunctionArn = lambdaFunctionArn {
                lambdaConfigurations.append(LambdaConfiguration(id: id, lambdaFunctionArn: lambdaFunctionArn, events: events, filter: nil))
            }
        }

        return NotificationConfiguration(
            topicConfigurations: topicConfigurations.isEmpty ? nil : topicConfigurations,
            queueConfigurations: queueConfigurations.isEmpty ? nil : queueConfigurations,
            lambdaConfigurations: lambdaConfigurations.isEmpty ? nil : lambdaConfigurations,
            webhookConfigurations: nil
        )
    }
}

// Helper to expose private content property from XMLBuilder because I defined it private but need it here.
// Actually, I defined it private in XMLBuilder struct, so I should expose a public property or method.
// I will assume for now I can edit XMLBuilder if needed, but wait, I defined 'content' as private var, but init populates it.
// I need to add a public getter for 'content' in XMLBuilder.
// Let me quickly fix XMLBuilder first to be safe.
