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

    static func listObjects(bucket: String, objects: [ObjectMetadata], prefix: String = "") -> String {
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
}
