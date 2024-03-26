package db;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class Pair {
    private final Optional<String> response;
    private final Optional<String> error;

    private Pair(Optional<String> response, Optional<String> error) {
        if (response.isEmpty() && error.isEmpty()) {
            throw new IllegalStateException("Both values can not be missing");
        }
        this.response = requireNonNull(response);
        this.error = requireNonNull(error);
    }

    public static Pair pair(Optional<String> response, Optional<String> error) {
        return new Pair(response, error);
    }

    public Pair map(Function<String, String> mapValue, Function<String, String> mapError) {
        return pair(response.map(mapValue), error.map(mapError));
    }

    public String actualValue() {
        return response.orElseGet(error::get);
    }
}
