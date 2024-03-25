import static java.util.Objects.requireNonNull;

public final class StreamKey {
    private final String key;
    private final String value;

    private StreamKey(String key, String value) {
        this.key = requireNonNull(key);
        this.value = requireNonNull(value);
    }

    public static StreamKey streamKey(String key, String value) {
        return new StreamKey(key, value);
    }

    public String key() {
        return key;
    }
}
