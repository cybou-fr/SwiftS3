import Foundation

struct XML {
    static func listBuckets(buckets: [(name: String, created: Date)]) -> String {
        let bucketEntries = buckets.map { bucket in
            """
            <Bucket>
                <Name>\(bucket.name)</Name>
                <CreationDate>\(ISO8601DateFormatter().string(from: bucket.created))</CreationDate>
            </Bucket>
            """
        }.joined()

        return """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListAllMyBucketsResult>
                <Buckets>
                    \(bucketEntries)
                </Buckets>
            </ListAllMyBucketsResult>
            """
    }

    static func listObjects(bucket: String, objects: [ObjectMetadata], prefix: String = "")
        -> String
    {
        let objectEntries = objects.map { object in
            """
            <Contents>
                <Key>\(object.key)</Key>
                <LastModified>\(ISO8601DateFormatter().string(from: object.lastModified))</LastModified>
                <ETag>&quot;\(object.eTag ?? "")&quot;</ETag>
                <Size>\(object.size)</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
            """
        }.joined()

        return """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>\(bucket)</Name>
                <Prefix></Prefix>
                <Marker></Marker>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
                \(objectEntries)
            </ListBucketResult>
            """
    }
    static func initiateMultipartUploadResult(bucket: String, key: String, uploadId: String)
        -> String
    {
        return """
            <?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Bucket>\(bucket)</Bucket>
                <Key>\(key)</Key>
                <UploadId>\(uploadId)</UploadId>
            </InitiateMultipartUploadResult>
            """
    }

    static func completeMultipartUploadResult(
        bucket: String, key: String, eTag: String, location: String
    ) -> String {
        return """
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Location>\(location)</Location>
                <Bucket>\(bucket)</Bucket>
                <Key>\(key)</Key>
                <ETag>"\(eTag)"</ETag>
            </CompleteMultipartUploadResult>
            """
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
}
