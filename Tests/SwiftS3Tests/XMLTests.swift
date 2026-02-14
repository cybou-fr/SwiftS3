import Foundation
import XCTest

@testable import SwiftS3

final class XMLTests: XCTestCase {

    func testListBuckets() {
        let buckets = [
            (name: "bucket1", created: Date(timeIntervalSince1970: 0)),
            (name: "bucket2", created: Date(timeIntervalSince1970: 86400))
        ]
        let xml = XML.listBuckets(buckets: buckets)
        XCTAssert(xml.contains("<Name>bucket1</Name>"))
        XCTAssert(xml.contains("<Name>bucket2</Name>"))
        XCTAssert(xml.contains("<ListAllMyBucketsResult>"))
    }

    func testListObjects() {
        let result = ListObjectsResult(
            objects: [
                ObjectMetadata(key: "key1", size: 100, lastModified: Date(timeIntervalSince1970: 0), eTag: "etag1")
            ],
            commonPrefixes: ["prefix1/"],
            isTruncated: false,
            nextMarker: "next"
        )
        let xml = XML.listObjects(bucket: "test-bucket", result: result, prefix: "", marker: "", maxKeys: 1000, isTruncated: false)
        XCTAssert(xml.contains("<Name>test-bucket</Name>"))
        XCTAssert(xml.contains("<Key>key1</Key>"))
        XCTAssert(xml.contains("<CommonPrefixes>"))
    }

    func testCopyObjectResult() {
        let metadata = ObjectMetadata(key: "key", size: 100, lastModified: Date(), eTag: "etag")
        let xml = XML.copyObjectResult(metadata: metadata)
        XCTAssert(xml.contains("CopyObjectResult"))
        XCTAssert(xml.contains("<ETag>&quot;etag&quot;</ETag>"))
    }

    func testInitiateMultipartUploadResult() {
        let xml = XML.initiateMultipartUploadResult(bucket: "bucket", key: "key", uploadId: "uploadId")
        XCTAssert(xml.contains("<Bucket>bucket</Bucket>"))
        XCTAssert(xml.contains("<UploadId>uploadId</UploadId>"))
    }

    func testCompleteMultipartUploadResult() {
        let xml = XML.completeMultipartUploadResult(bucket: "bucket", key: "key", eTag: "etag", location: "location")
        XCTAssert(xml.contains("<Location>location</Location>"))
        XCTAssert(xml.contains("<ETag>&quot;etag&quot;</ETag>"))
    }

    func testParseCompleteMultipartUpload() {
        let xml = """
        <CompleteMultipartUpload>
            <Part>
                <PartNumber>1</PartNumber>
                <ETag>"etag1"</ETag>
            </Part>
            <Part>
                <PartNumber>2</PartNumber>
                <ETag>"etag2"</ETag>
            </Part>
        </CompleteMultipartUpload>
        """
        let parts = XML.parseCompleteMultipartUpload(xml: xml)
        XCTAssertEqual(parts.count, 2)
        XCTAssertEqual(parts[0].partNumber, 1)
        XCTAssertEqual(parts[0].eTag, "etag1")
    }

    func testDeleteResult() {
        let xml = XML.deleteResult(deleted: [("key1", nil, false, nil)], errors: [("key2", "code", "message")])
        XCTAssert(xml.contains("<Deleted>"))
        XCTAssert(xml.contains("<Error>"))
    }

    func testParseDeleteObjects() {
        let xml = """
        <Delete>
            <Object><Key>key1</Key></Object>
            <Object><Key>key2</Key></Object>
        </Delete>
        """
        let objects = XML.parseDeleteObjects(xml: xml)
        XCTAssertEqual(objects, [DeleteObject(key: "key1", versionId: nil), DeleteObject(key: "key2", versionId: nil)])
    }

    func testAccessControlPolicy() {
        let policy = AccessControlPolicy(
            owner: Owner(id: "ownerId", displayName: "ownerName"),
            accessControlList: [
                Grant(
                    grantee: Grantee(id: "granteeId", displayName: "granteeName", type: "CanonicalUser"),
                    permission: .fullControl
                )
            ]
        )
        let xml = XML.accessControlPolicy(policy: policy)
        XCTAssert(xml.contains("<Owner>"))
        XCTAssert(xml.contains("<ID>ownerId</ID>"))
        XCTAssert(xml.contains("<Grant>"))
    }

    func testVersioningConfiguration() {
        let config = VersioningConfiguration(status: .enabled)
        let xml = XML.versioningConfiguration(config: config)
        XCTAssert(xml.contains("<Status>Enabled</Status>"))
    }

    func testTaggingConfiguration() {
        let tags = [S3Tag(key: "key", value: "value")]
        let xml = XML.taggingConfiguration(tags: tags)
        XCTAssert(xml.contains("<TagSet>"))
        XCTAssert(xml.contains("<Key>key</Key>"))
    }

    func testParseTagging() {
        let xml = """
        <Tagging>
            <TagSet>
                <Tag>
                    <Key>key1</Key>
                    <Value>value1</Value>
                </Tag>
            </TagSet>
        </Tagging>
        """
        let tags = XML.parseTagging(xml: xml)
        XCTAssertEqual(tags.count, 1)
        XCTAssertEqual(tags[0].key, "key1")
        XCTAssertEqual(tags[0].value, "value1")
    }

    func testLifecycleConfiguration() {
        let rule = LifecycleConfiguration.Rule(
            id: "rule1",
            status: .enabled,
            filter: LifecycleConfiguration.Rule.Filter(prefix: "prefix"),
            expiration: LifecycleConfiguration.Rule.Expiration(days: 30),
            noncurrentVersionExpiration: nil
        )
        let config = LifecycleConfiguration(rules: [rule])
        let xml = XML.lifecycleConfiguration(config: config)
        XCTAssert(xml.contains("<ID>rule1</ID>"))
        XCTAssert(xml.contains("<Days>30</Days>"))
    }

    func testParseLifecycle() {
        let xml = """
        <LifecycleConfiguration>
            <Rule>
                <ID>rule1</ID>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix>prefix</Prefix>
                </Filter>
                <Expiration>
                    <Days>30</Days>
                </Expiration>
            </Rule>
        </LifecycleConfiguration>
        """
        let config = XML.parseLifecycle(xml: xml)
        XCTAssertEqual(config.rules.count, 1)
        XCTAssertEqual(config.rules[0].id, "rule1")
        XCTAssertEqual(config.rules[0].expiration?.days, 30)
    }
}