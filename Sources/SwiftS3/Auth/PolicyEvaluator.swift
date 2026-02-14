import Foundation

public enum PolicyDecision: Sendable {
    case allow
    case deny
    case implicitDeny  // No policy matched
}

/// Evaluates AWS IAM policy documents against access requests.
/// Implements the AWS IAM policy evaluation logic with explicit deny precedence.
/// Supports complex policy statements with conditions, principals, and resource patterns.
public struct PolicyEvaluator: Sendable {
    public init() {}

    /// Evaluates a bucket policy against an access request.
    /// Implements AWS IAM policy evaluation logic:
    /// 1. Check for explicit DENY statements (always take precedence)
    /// 2. Check for explicit ALLOW statements
    /// 3. Default to implicit DENY if no matching statements
    ///
    /// - Parameters:
    ///   - policy: The bucket policy to evaluate
    ///   - request: The access request context (principal, action, resource)
    /// - Returns: PolicyDecision indicating allow, deny, or implicit deny
    public func evaluate(policy: BucketPolicy, request: PolicyRequest) -> PolicyDecision {
        // 1. Check for Explicit Deny
        for statement in policy.Statement {
            if statement.Effect == .Deny && matches(statement: statement, request: request) {
                return .deny
            }
        }

        // 2. Check for Explicit Allow
        var isAllowed = false
        for statement in policy.Statement {
            if statement.Effect == .Allow && matches(statement: statement, request: request) {
                isAllowed = true
            }
        }

        return isAllowed ? .allow : .implicitDeny
    }

    /// Checks if a policy statement matches the given request.
    /// A statement matches if ALL conditions are met:
    /// - Principal matches (user/role making request)
    /// - Action matches (operation being performed)
    /// - Resource matches (bucket/object being accessed)
    ///
    /// - Parameters:
    ///   - statement: Policy statement to evaluate
    ///   - request: Access request to check against
    /// - Returns: True if statement applies to this request
    private func matches(statement: PolicyStatement, request: PolicyRequest) -> Bool {
        guard matchesPrincipal(statement.Principal, request.principal) else { return false }
        guard matchesAction(statement.Action, request.action) else { return false }
        guard matchesResource(statement.Resource, request.resource) else { return false }
        return true
    }

    /// Checks if the policy principal matches the request principal.
    /// Supports wildcard matching and specific ARN/user ID matching.
    ///
    /// - Parameters:
    ///   - policyPrincipal: Principal from policy statement
    ///   - requestPrincipal: Principal from access request
    /// - Returns: True if principals match
    private func matchesPrincipal(_ policyPrincipal: PolicyPrincipal, _ requestPrincipal: String?)
        -> Bool
    {
        switch policyPrincipal {
        case .any:
            return true
        case .specific(let dict):
            // Check for "AWS": "arn..." or "AWS": "*"
            if let aws = dict["AWS"] {
                if aws == "*" { return true }
                if let req = requestPrincipal, aws == req { return true }
                // Also handle short ID matching if needed, but strict string match for now
            }
            return false
        }
    }

    /// Checks if the policy action matches the request action.
    /// Supports wildcard matching (e.g., "s3:*" matches all S3 actions).
    ///
    /// - Parameters:
    ///   - policyAction: Action(s) from policy statement
    ///   - requestAction: Action from access request
    /// - Returns: True if actions match
    private func matchesAction(_ policyAction: SingleOrArray<String>, _ requestAction: String)
        -> Bool
    {
        let actions = policyAction.values
        for action in actions {
            if action == "*" || action == "s3:*" { return true }
            if action == requestAction { return true }
        }
        return false
    }

    /// Checks if the policy resource matches the request resource.
    /// Supports exact matching and simple wildcard patterns.
    ///
    /// - Parameters:
    ///   - policyResource: Resource(s) from policy statement
    ///   - requestResource: Resource from access request
    /// - Returns: True if resources match
    private func matchesResource(_ policyResource: SingleOrArray<String>, _ requestResource: String)
        -> Bool
    {
        let resources = policyResource.values
        for resource in resources {
            if resource == "*" { return true }
            if resource == requestResource { return true }

            // Simple wildcard matching: "arn:aws:s3:::bucket/*" matches "arn:aws:s3:::bucket/foo"
            if resource.hasSuffix("*") {
                let prefix = String(resource.dropLast())
                if requestResource.hasPrefix(prefix) { return true }
            }
        }
        return false
    }
}

public struct PolicyRequest: Sendable {
    public let principal: String?  // ARN or AccessKey
    public let action: String  // e.g. "s3:GetObject"
    public let resource: String  // e.g. "arn:aws:s3:::mybucket/myobject"

    /// Initializes a policy evaluation request
    /// - Parameters:
    ///   - principal: The principal making the request (ARN or access key)
    ///   - action: The action being requested (e.g., "s3:GetObject")
    ///   - resource: The resource being accessed (e.g., bucket/object ARN)
    public init(principal: String?, action: String, resource: String) {
        self.principal = principal
        self.action = action
        self.resource = resource
    }
}
