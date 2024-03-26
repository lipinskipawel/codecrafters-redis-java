package db;

import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

record Entries(String id, Map<String, String> pairs) {
    Entries {
        requireNonNull(id);
        requireNonNull(pairs);
    }

    long millis() {
        return parseLong(id.split("-")[0]);
    }

    int sequenceNumber() {
        return parseInt(id.split("-")[1]);
    }
}
