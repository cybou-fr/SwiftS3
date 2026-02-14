import Foundation

/// A utility for building XML documents with proper formatting and escaping.
/// Designed specifically for generating AWS S3-compatible XML responses.
/// Provides a fluent API for constructing XML elements with attributes and nested content.
/// Automatically handles XML escaping and proper formatting for S3 API compatibility.
struct XMLBuilder {
    /// The generated XML content as a string.
    public private(set) var content: String = ""

    /// Initializes an XML document with a root element.
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

    /// Creates a simple XML element with text content.
    /// Automatically escapes XML special characters in the value.
    ///
    /// - Parameters:
    ///   - name: XML element name
    ///   - value: Text content to be escaped and inserted
    /// - Returns: Formatted XML element string with proper indentation
    static func element(_ name: String, _ value: String) -> String {
        return "    <\(name)>\(escape(value))</\(name)>\n"
    }

    /// Creates an XML element with attributes and nested content.
    /// Supports complex XML structures with attributes and child elements.
    ///
    /// - Parameters:
    ///   - name: XML element name
    ///   - attributes: Dictionary of attribute name-value pairs
    ///   - content: Closure that returns nested XML content
    /// - Returns: Formatted XML element string with proper indentation
    static func element(_ name: String, attributes: [String: String] = [:], content: () -> String)
        -> String
    {
        let attrs = formatAttributes(attributes)
        return "    <\(name)\(attrs)>\n\(content())    </\(name)>\n"
    }

    /// Formats XML attributes into a string for inclusion in element tags
    /// - Parameter attributes: Dictionary of attribute name-value pairs
    /// - Returns: Space-separated string of name="value" pairs, or empty string if no attributes
    private static func formatAttributes(_ attributes: [String: String]) -> String {
        guard !attributes.isEmpty else { return "" }
        return " " + attributes.map { "\($0.key)=\"\($0.value)\"" }.joined(separator: " ")
    }

    /// Formats XML attributes into a string for inclusion in element tags (instance method)
    /// - Parameter attributes: Dictionary of attribute name-value pairs
    /// - Returns: Space-separated string of name="value" pairs, or empty string if no attributes
    private func formatAttributes(_ attributes: [String: String]) -> String {
        return XMLBuilder.formatAttributes(attributes)
    }

    /// Escapes special XML characters in a string for safe inclusion in XML content
    /// - Parameter string: The string to escape
    /// - Returns: XML-safe string with &, <, >, and " characters escaped
    private static func escape(_ string: String) -> String {
        return
            string
            .replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
            .replacingOccurrences(of: "\"", with: "&quot;")
            .replacingOccurrences(of: "'", with: "&apos;")
    }

    /// Escapes special XML characters in a string for safe inclusion in XML content (instance method)
    /// - Parameter string: The string to escape
    /// - Returns: XML-safe string with &, <, >, and " characters escaped
    private func escape(_ string: String) -> String {
        return XMLBuilder.escape(string)
    }
}
