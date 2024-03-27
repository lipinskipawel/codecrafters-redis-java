package db;

import java.util.Map;
import java.util.Stack;

import static db.Pair.pair;
import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.util.HashMap.newHashMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

final class StreamStore {
    private final Map<String, Stack<Entries>> streams;

    private StreamStore(Map<String, Stack<Entries>> streams) {
        this.streams = requireNonNull(streams);
    }

    public static StreamStore streamStore() {
        return new StreamStore(newHashMap(16));
    }

    public Pair put(String streamKey, String value, Map<String, String> map) {
        final var entries = streams.get(streamKey);

        if (value.equals("*")) {
            final var stack = new Stack<Entries>();
            final var millis = currentTimeMillis() + "-0";
            stack.push(new Entries(millis, map));
            streams.put(streamKey, stack);
            return pair(of(millis), empty());
        }

        final var id = value.split("-");
        final var millis = parseLong(id[0]);

        if (id[1].equals("*")) {
            return generateSequenceNumber(millis, streamKey, map, entries);
        }

        if (entries == null) {
            final var stack = new Stack<Entries>();
            stack.push(new Entries(value, map));
            streams.put(streamKey, stack);
            return pair(of(value), empty());
        }

        final var sequenceNumber = parseLong(id[1]);

        if (millis == 0 && sequenceNumber == 0) {
            return pair(empty(), of("ERR The ID specified in XADD must be greater than 0-0"));
        }

        final var head = entries.peek();
        final var headMillis = head.millis();
        final var headSequenceNumber = head.sequenceNumber();

        if (millis == headMillis) {
            if (sequenceNumber <= headSequenceNumber) {
                return pair(empty(), of("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
            }
            entries.push(new Entries(value, map));
            return pair(of(value), empty());
        }

        if (millis < headMillis) {
            return pair(empty(), of("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }

        if (sequenceNumber <= headSequenceNumber) {
            return pair(empty(), of("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }

        entries.push(new Entries(value, map));
        return pair(of(value), empty());
    }

    private Pair generateSequenceNumber(
            long millis,
            String streamKey,
            Map<String, String> map,
            Stack<Entries> entries
    ) {
        if (millis == 0) {
            final var generatedId = "0-1";
            final var stack = new Stack<Entries>();
            stack.push(new Entries(generatedId, map));
            streams.put(streamKey, stack);
            return pair(of(generatedId), empty());
        }
        for (var entry : entries) {
            if (entry.millis() == millis) {
                final var generated = entry.sequenceNumber() + 1;
                final var generatedId = millis + "-" + generated;
                entries.push(new Entries(generatedId, map));
                return pair(of(generatedId), empty());
            }
        }
        final var generatedId = millis + "-" + "0";
        final var stack = new Stack<Entries>();
        stack.push(new Entries(generatedId, map));
        streams.put(streamKey, stack);
        return pair(of(generatedId), empty());
    }

    public Stack<Entries> range(String streamKey, String start, String end) {
        final var entries = streams.get(streamKey);
        if (entries == null) {
            return new Stack<>();
        }
        final var startTime = start.split("-")[0];
        final var startSequenceNumber = parseLong(start.split("-")[1]);
        final var endTime = end.split("-")[0];
        final var endSequenceNumber = parseLong(end.split("-")[1]);
        final var result = new Stack<Entries>();
        for (var entry : entries) {
            final var split = entry.id().split("-");
            final var time = split[0];
            final var sequenceNumber = parseLong(split[1]);
            if (startTime.equals(time) &&
                    (sequenceNumber >= startSequenceNumber && sequenceNumber <= endSequenceNumber)
            ) {
                result.push(entry);
            }
        }
        return result;
    }

    public boolean containsStream(String key) {
        return streams.containsKey(key);
    }
}
