import Foundation
import Hummingbird
import Logging
import NIO

@dynamicMemberLookup
struct S3RequestContext: RequestContext {
    var coreContext: CoreRequestContextStorage
    var principal: String?

    init(source: Source) {
        self.coreContext = .init(source: source)
        self.principal = nil
    }

    subscript<T>(dynamicMember keyPath: KeyPath<CoreRequestContextStorage, T>) -> T {
        return coreContext[keyPath: keyPath]
    }
}
