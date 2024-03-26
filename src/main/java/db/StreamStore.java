package db;

import java.util.Map;
import java.util.Stack;

import static java.lang.Long.parseLong;
import static java.util.HashMap.newHashMap;
import static java.util.Objects.requireNonNull;

final class StreamStore {
    private final Map<String, Stack<Entries>> streams;

    private StreamStore(Map<String, Stack<Entries>> streams) {
        this.streams = requireNonNull(streams);
    }

    public static StreamStore streamStore() {
        return new StreamStore(newHashMap(16));
    }

    public String put(String streamKey, String value, Map<String, String> map) {
        final var entries = streams.get(streamKey);
        final var id = value.split("-");
        final var millis = parseLong(id[0]);
        final var sequenceNumber = parseLong(id[1]);

        if (entries == null) {
            final var stack = new Stack<Entries>();
            stack.push(new Entries(value, map));
            streams.put(streamKey, stack);
            return value;
        }

        final var head = entries.peek();
        final var headMillis = head.millis();
        final var headSequenceNumber = head.sequenceNumber();

        if (millis == 0 && sequenceNumber == 0) {
            return "ERR The ID specified in XADD must be greater than 0-0";
        }

        if (millis == headMillis) {
            if (sequenceNumber <= headSequenceNumber) {
                return "ERR The ID specified in XADD is equal or smaller than the target stream top item";
            }
            entries.push(new Entries(value, map));
            return value;
        }

        if (millis < headMillis) {
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item";
        }

        if (sequenceNumber <= headSequenceNumber) {
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item";
        }

        entries.push(new Entries(value, map));
        return value;
    }

    public boolean containsStream(String key) {
        return streams.containsKey(key);
    }
}
