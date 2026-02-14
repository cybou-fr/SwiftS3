import Foundation
import Hummingbird
import Logging
import NIO

/// Request context for S3 API operations, extending Hummingbird's RequestContext.
/// Provides access to request-specific data and authenticated principal information.
@dynamicMemberLookup
struct S3RequestContext: RequestContext {
    var coreContext: CoreRequestContextStorage
    /// The authenticated principal (access key) for the current request.
    var principal: String?

    /// Initializes a new S3RequestContext with the given source
    /// - Parameter source: The source for the core request context
    init(source: Source) {
        self.coreContext = .init(source: source)
        self.principal = nil
    }

    subscript<T>(dynamicMember keyPath: KeyPath<CoreRequestContextStorage, T>) -> T {
        return coreContext[keyPath: keyPath]
    }
}
