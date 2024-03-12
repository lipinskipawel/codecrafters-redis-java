package resp;

import resp.Command.BulkString;
import resp.Command.SimpleString;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.util.Optional.empty;
import static java.util.Optional.of;

public final class Decoder {

    public Optional<SimpleString> decodeSimpleString(BufferedReader reader) {
        try {
            return of(new SimpleString(reader.readLine()));
        } catch (IOException ioException) {
            return empty();
        }
    }

    public Optional<BulkString> decodeBulkString(BufferedReader reader) {
        try {
            final var dollarLine = reader.readLine();
            final var numberOfBytes = parseInt(dollarLine.substring(1));
            final var data = reader.readLine();
            return of(new BulkString(data));
        } catch (IOException ioException) {
            return empty();
        }
    }
}
