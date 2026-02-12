#!/usr/bin/env swift

import Foundation

// Simple test script to verify bucket name validation
func isValidBucketName(_ name: String) -> Bool {
    // Bucket names must be between 3 and 63 characters long
    guard (3...63).contains(name.count) else { return false }

    // Bucket names can consist only of lowercase letters, numbers, hyphens, and periods
    let allowedCharacters = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyz0123456789-.")
    guard name.unicodeScalars.allSatisfy({ allowedCharacters.contains($0) }) else { return false }

    // Bucket names must begin and end with a letter or number
    guard let first = name.first, let last = name.last else { return false }
    let alphanumeric = CharacterSet.alphanumerics
    guard alphanumeric.contains(first.unicodeScalars.first!) &&
          alphanumeric.contains(last.unicodeScalars.first!) else { return false }

    // Bucket names cannot contain two adjacent periods
    guard !name.contains("..") else { return false }

    // Bucket names cannot be formatted as an IP address
    let ipPattern = #"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"#
    guard name.range(of: ipPattern, options: .regularExpression) == nil else { return false }

    return true
}

// Test cases from ErrorPathTests
let invalidNames = [
    "a",           // too short
    "ab",          // too short
    "invalid..bucket", // consecutive periods
    "192.168.1.1", // IP address format
    "invalid_bucket", // underscore not allowed
    "InvalidBucket", // uppercase not allowed
    "bucket-",     // ends with hyphen
    "-bucket",     // starts with hyphen
    "bucket.",     // ends with period
    ".bucket",     // starts with period
    "bucket name", // space not allowed
    "bucket@name", // @ not allowed
]

let validNames = [
    "valid-bucket-name",
    "valid.bucket.name",
    "valid123bucket",
    "123validbucket",
    "bucket123",
    "my-bucket-123.test"
]

print("Testing invalid bucket names:")
for name in invalidNames {
    let result = isValidBucketName(name)
    print("  '\(name)' -> \(result ? "VALID (ERROR)" : "INVALID (OK)")")
}

print("\nTesting valid bucket names:")
for name in validNames {
    let result = isValidBucketName(name)
    print("  '\(name)' -> \(result ? "VALID (OK)" : "INVALID (ERROR)")")
}