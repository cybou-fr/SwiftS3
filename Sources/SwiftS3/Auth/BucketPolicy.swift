import Foundation

public struct BucketPolicy: Codable, Sendable {
    public let Version: String
    public let Id: String?
    public let Statement: [PolicyStatement]

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
        if let dict = try? container.decode([String: [String]].self) {
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

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .single(let val):
            try container.encode(val)
        case .array(let arr):
            try container.encode(arr)
        }
    }

    public var values: [T] {
        switch self {
        case .single(let v): return [v]
        case .array(let a): return a
        }
    }
}
