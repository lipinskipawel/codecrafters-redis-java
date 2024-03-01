import java.util.Optional;

final class Encoder {

    public String encodeAsSimpleString(String toEncode) {
        return "+" + toEncode + "\r\n";
    }

    public String encodeAsBulkString(String toEncode) {
        final var length = "$" + toEncode.length() + "\r\n";
        return length + toEncode + "\r\n";
    }

    public String encodeAsBulkString(Optional<String> toEncode) {
        return toEncode
                .map(this::encodeAsBulkString)
                .orElse("$-1\r\n");
    }
}
