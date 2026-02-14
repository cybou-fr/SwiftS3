import Foundation

public struct BucketPolicy: Codable, Sendable {
    public let Version: String
    public let Id: String?
    public let Statement: [PolicyStatement]

    /// Initializes a new bucket policy
    /// - Parameters:
    ///   - Version: Policy version (defaults to "2012-10-17")
    ///   - Id: Optional policy ID
    ///   - Statement: Array of policy statements defining permissions
    public init(Version: String = "2012-10-17", Id: String? = nil, Statement: [PolicyStatement]) {
        self.Version = Version
        self.Id = Id
        self.Statement = Statement
    }
}

public struct PolicyStatement: Codable, Sendable {
    public let Sid: String?
    public let Effect: StatementEffect
    public let Principal: PolicyPrincipal
    public let Action: SingleOrArray<String>
    public let Resource: SingleOrArray<String>

    /// Initializes a new policy statement
    /// - Parameters:
    ///   - Sid: Optional statement ID for identification
    ///   - Effect: Allow or Deny effect for the statement
    ///   - Principal: The principal(s) this statement applies to
    ///   - Action: The action(s) being allowed or denied
    ///   - Resource: The resource(s) this statement applies to
    public init(
        Sid: String? = nil, Effect: StatementEffect, Principal: PolicyPrincipal,
        Action: SingleOrArray<String>, Resource: SingleOrArray<String>
    ) {
        self.Sid = Sid
        self.Effect = Effect
        self.Principal = Principal
        self.Action = Action
        self.Resource = Resource
    }
}

public enum StatementEffect: String, Codable, Sendable {
    case Allow
    case Deny
}

public enum PolicyPrincipal: Codable, Sendable {
    case any
    case specific([String: String])  // e.g. ["AWS": "arn:aws:iam::123:user/bob"]

    /// Decodes a PolicyPrincipal from JSON, supporting "*" for any principal or object notation
    /// - Parameter decoder: The decoder to read data from
    /// - Throws: DecodingError if the principal format is invalid
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let str = try? container.decode(String.self), str == "*" {
            self = .any
            return
        }
        if let dict = try? container.decode([String: String].self) {
            self = .specific(dict)
            return
        }
        if let _ = try? container.decode([String: [String]].self) {
            // Handle array of principals case if needed, mapping to specific
            // For now simplified to single string map or *
            // Actually AWS Principal can be slightly complex: {"AWS": ["arn1", "arn2"]}
            // Let's stick to simple map [String: String] for v1 or error
            throw DecodingError.dataCorruptedError(
                in: container, debugDescription: "Principal must be '*' or object")
        }
        throw DecodingError.dataCorruptedError(
            in: container, debugDescription: "Principal must be '*' or object")
    }

    /// Encodes the PolicyPrincipal to JSON, using "*" for any principal or object notation for specific principals
    /// - Parameter encoder: The encoder to write data to
    /// - Throws: EncodingError if encoding fails
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .any:
            try container.encode("*")
        case .specific(let dict):
            try container.encode(dict)
        }
    }
}

public enum SingleOrArray<T: Codable & Sendable>: Codable, Sendable {
    case single(T)
    case array([T])

    /// Decodes either a single value or an array of values from JSON
    /// - Parameter decoder: The decoder to read data from
    /// - Throws: DecodingError if the data is neither a single value nor an array
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let val = try? container.decode(T.self) {
            self = .single(val)
            return
        }
        if let arr = try? container.decode([T].self) {
            self = .array(arr)
            return
        }
        throw DecodingError.dataCorruptedError(
            in: container, debugDescription: "Expected single value or array")
    }

    /// Encodes the SingleOrArray to JSON as either a single value or an array
    /// - Parameter encoder: The encoder to write data to
    /// - Throws: EncodingError if encoding fails
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .single(let val):
            try container.encode(val)
        case .array(let arr):
            try container.encode(arr)
        }
    }

    /// Returns all values as an array, normalizing single values into single-element arrays
    public var values: [T] {
        switch self {
        case .single(let v): return [v]
        case .array(let a): return a
        }
    }
}
