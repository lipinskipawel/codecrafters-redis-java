import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static java.time.Duration.ofMinutes;
import static java.time.Instant.now;
import static java.util.HashMap.newHashMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

final class Database {
    private final Map<String, String> simpleKeyValue;
    private final Map<String, Instant> expiryKey;

    public Database() {
        this.simpleKeyValue = newHashMap(16);
        this.expiryKey = newHashMap(16);
    }

    public synchronized void set(String key, String value) {
        requireNonNull(key);
        requireNonNull(value);
        simpleKeyValue.put(key, value);
    }

    public synchronized void set(String key, String value, Duration duration) {
        requireNonNull(key);
        requireNonNull(value);
        requireNonNull(duration);
        simpleKeyValue.put(key, value);
        expiryKey.put(key, now().plus(duration));
    }

    public synchronized Optional<String> get(String key) {
        requireNonNull(key);
        final var expiryTime = expiryKey.getOrDefault(key, now().plus(ofMinutes(2)));
        if (now().isAfter(expiryTime)) {
            simpleKeyValue.remove(key);
            expiryKey.remove(key);
            return empty();
        }
        return ofNullable(simpleKeyValue.get(key));
    }
}
