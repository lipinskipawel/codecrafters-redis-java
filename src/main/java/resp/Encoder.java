package resp;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public final class Encoder {

    public String encodeAsSimpleString(String toEncode) {
        return "+" + toEncode + "\r\n";
    }

    public String encodeAsBulkString(String toEncode) {
        final var firstRow = "$" + toEncode.length() + "\r\n";
        return firstRow + toEncode + "\r\n";
    }

    public String encodeAsBulkString(Optional<String> toEncode) {
        return toEncode
                .map(this::encodeAsBulkString)
                .orElse("$-1\r\n");
    }

    public String encodeAsBulkString(List<String> toEncode) {
        final var data = toEncode.stream()
                .map(it -> it + "\r\n")
                .reduce(new StringBuilder(), StringBuilder::append, StringBuilder::append)
                .toString();
        final var firstRow = "$" + data.length() + "\r\n";
        return firstRow + data + "\r\n";
    }

    public String encodeAsArray(String toEncode) {
        return encodeAsArray(List.of(toEncode));
    }

    public String encodeAsArray(List<String> toEncode) {
        final var firstRow = "*" + toEncode.size() + "\r\n";
        return firstRow + toEncode.stream()
                .map(this::encodeAsBulkString)
                .collect(joining());
    }

    public String encodeAsInteger(long integer) {
        return ":" + integer + "\r\n";
    }
}
