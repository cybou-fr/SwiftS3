import Foundation

struct XMLBuilder {
    public private(set) var content: String = ""

    init(root: String, attributes: [String: String] = [:], content: () -> String) {
        self.content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        self.content += "<\(root)\(formatAttributes(attributes))>\n"
        self.content += content()
        self.content += "</\(root)>"
    }

    private init(
        tag: String, attributes: [String: String] = [:], value: String? = nil,
        content: (() -> String)? = nil
    ) {
        var openTag = "<\(tag)\(formatAttributes(attributes))>"
        if let value = value {
            openTag += escape(value)
            openTag += "</\(tag)>"
            self.content = openTag + "\n"
        } else if let content = content {
            openTag += "\n" + content() + "</\(tag)>"
            self.content = openTag + "\n"
        } else {
            // Self-closing or empty? S3 usually prefers explicit closing.
            self.content = openTag + "</\(tag)>\n"
        }
    }

    static func element(_ name: String, _ value: String) -> String {
        return "    <\(name)>\(escape(value))</\(name)>\n"
    }

    static func element(_ name: String, attributes: [String: String] = [:], content: () -> String)
        -> String
    {
        let attrs = formatAttributes(attributes)
        return "    <\(name)\(attrs)>\n\(content())    </\(name)>\n"
    }

    // Helper to format attributes
    private static func formatAttributes(_ attributes: [String: String]) -> String {
        guard !attributes.isEmpty else { return "" }
        return " " + attributes.map { "\($0.key)=\"\($0.value)\"" }.joined(separator: " ")
    }

    private func formatAttributes(_ attributes: [String: String]) -> String {
        return XMLBuilder.formatAttributes(attributes)
    }

    // Basic XML Escaping
    private static func escape(_ string: String) -> String {
        return
            string
            .replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
            .replacingOccurrences(of: "\"", with: "&quot;")
            .replacingOccurrences(of: "'", with: "&apos;")
    }

    // For instance method access if needed, though static is easier for functional composition
    private func escape(_ string: String) -> String {
        return XMLBuilder.escape(string)
    }
}
