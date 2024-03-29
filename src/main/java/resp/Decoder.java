package resp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.util.Optional.empty;
import static java.util.Optional.of;

public final class Decoder {

    public Optional<String> decodeSimpleString(BufferedReader reader) {
        try {
            return of(reader.readLine());
        } catch (IOException ioException) {
            return empty();
        }
    }

    /**
     * Clients always sends array of String's
     * https://redis.io/docs/reference/protocol-spec/#sending-commands-to-a-redis-server
     * https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
     *
     * @param reader BufferedReader
     * @return parsed Command
     */
    public Optional<Command> parseCommand(BufferedReader reader) {
        try {
            while (!reader.ready()) {
            }
            return of(decodeAsArrayResp(reader));
        } catch (IOException ioException) {
            return empty();
        }
    }

    private Command decodeAsArrayResp(BufferedReader reader) {
        try {
            final var arrayLength = parseInt(reader.readLine().substring(1));
            if (arrayLength == 0) {
                throw new IllegalArgumentException("Client just sent empty array");
            }

            final var elements = new ArrayList<String>();
            for (var i = 0; i < arrayLength; i++) {
                final var element = decodeBulkString(reader).orElseThrow();
                elements.add(element);
            }
            return switch (elements.get(0).toLowerCase()) {
                case "ping" -> new Command.Ping(elements.get(0));
                case "echo" -> new Command.Echo(elements.get(0), elements.get(1));
                case "set" -> {
                    if (elements.size() == 3) {
                        yield new Command.Set(elements.get(0), elements.get(1), elements.get(2), empty());
                    }
                    yield new Command.Set(elements.get(0), elements.get(1), elements.get(2), of(elements.get(4)));
                }
                case "get" -> new Command.Get(elements.get(0), elements.get(1));
                case "info" -> new Command.Info(elements.get(0), elements.get(1));
                case "replconf" -> new Command.Replconf(elements.get(0), elements.get(1), elements.get(2));
                case "psync" -> new Command.Psync(elements.get(0), elements.get(1), elements.get(2));
                case "wait" -> new Command.Wait(elements.get(0), elements.get(1), elements.get(2));
                case "config" -> new Command.Config(elements.get(0), elements.get(1), elements.get(2));
                case "type" -> new Command.Type(elements.get(0), elements.get(1));
                case "xadd" -> {
                    final var keyValues = elements.stream()
                            .skip(3)
                            .toList();
                    final var map = new HashMap<String, String>();
                    for (var i = 0; i < keyValues.size(); i = i + 2) {
                        map.put(keyValues.get(i), keyValues.get(i + 1));
                    }
                    yield new Command.Xadd(elements.get(0), elements.get(1), elements.get(2), map);
                }
                case "xrange" -> new Command.Xrange(elements.get(0), elements.get(1), elements.get(2), elements.get(3));
                case "xread" -> {
                    final var block = findBlock(elements);
                    final var skip = block.isEmpty() ? 2 : 4;
                    final var keyValues = elements.stream()
                            .skip(skip)
                            .toList();
                    final var step = keyValues.size() / 2;
                    final Map<String, String> map = new LinkedHashMap<>();
                    for (var i = 0; i < keyValues.size() / 2; i++) {
                        map.put(keyValues.get(i), keyValues.get(i + step));
                    }
                    yield new Command.Xread(elements.get(0), block, map);
                }
                default -> throw new IllegalStateException("Unexpected value: " + elements.get(0));
            };
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    private Optional<String> findBlock(List<String> xread) {
        var blockTime = Optional.<String>empty();
        if (xread.get(1).equals("block")) {
            blockTime = of(xread.get(2));
        }
        return blockTime;
    }

    private Optional<String> decodeBulkString(BufferedReader reader) {
        try {
            final var dollarLine = reader.readLine();
            final var numberOfBytes = parseInt(dollarLine.substring(1));
            final var data = reader.readLine();
            return of(data);
        } catch (IOException ioException) {
            return empty();
        }
    }
}
