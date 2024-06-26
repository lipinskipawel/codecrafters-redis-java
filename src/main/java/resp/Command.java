package resp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public sealed interface Command {
    List<String> elements();

    record Ping(String commandType) implements Command {
        public Ping {
            requireNonNull(commandType);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType);
        }
    }

    record Echo(String commandType, String argument) implements Command {
        public Echo {
            requireNonNull(commandType);
            requireNonNull(argument);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, argument);
        }
    }

    record Set(String commandType, String key, String value, Optional<String> expiryTime) implements Command {
        public Set {
            requireNonNull(commandType);
            requireNonNull(key);
            requireNonNull(value);
            requireNonNull(expiryTime);
        }

        @Override
        public List<String> elements() {
            return expiryTime
                    .map(it -> List.of(commandType, key, value, it))
                    .or(() -> Optional.of(List.of(commandType, key, value)))
                    .get();
        }
    }

    record Get(String commandType, String value) implements Command {
        public Get {
            requireNonNull(commandType);
            requireNonNull(value);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, value);
        }
    }

    record Info(String commandType, String section) implements Command {
        public Info {
            requireNonNull(commandType);
            requireNonNull(section);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType);
        }
    }

    record Replconf(String commandType, String key, String value) implements Command {
        public Replconf {
            requireNonNull(commandType);
            requireNonNull(key);
            requireNonNull(value);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, key, value);
        }
    }

    record Psync(String commandType, String replicationId, String offset) implements Command {
        public Psync {
            requireNonNull(commandType);
            requireNonNull(replicationId);
            requireNonNull(offset);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, replicationId, offset);
        }
    }

    record Wait(String commandType, String numberOfReplica, String timeout) implements Command {
        public Wait {
            requireNonNull(commandType);
            requireNonNull(numberOfReplica);
            requireNonNull(timeout);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, numberOfReplica, timeout);
        }
    }

    record Config(String commandType, String key, String value) implements Command {
        public Config {
            requireNonNull(commandType);
            requireNonNull(key);
            requireNonNull(value);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, key, value);
        }
    }

    record Type(String commandType, String key) implements Command {
        public Type {
            requireNonNull(commandType);
            requireNonNull(key);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, key);
        }
    }

    record Xadd(String commandType, String streamKey, String streamKeyValue, Map<String, String> values)
            implements Command {
        public Xadd {
            requireNonNull(commandType);
            requireNonNull(streamKey);
            requireNonNull(streamKeyValue);
            requireNonNull(values);
        }

        @Override
        public List<String> elements() {
            final var mapValues = values.entrySet()
                    .stream()
                    .flatMap(it -> Stream.of(it.getKey(), it.getValue()));
            return concat(Stream.of(commandType, streamKey, streamKeyValue), mapValues).toList();
        }
    }

    record Xrange(String commandType, String streamKey, String start, String end) implements Command {
        public Xrange {
            requireNonNull(commandType);
            requireNonNull(streamKey);
            requireNonNull(start);
            requireNonNull(end);
        }

        @Override
        public List<String> elements() {
            return List.of(commandType, streamKey, start, end);
        }
    }

    record Xread(String commandType, Optional<String> blockTime, Map<String, String> streamKeyWithId)
            implements Command {
        public Xread {
            requireNonNull(commandType);
            requireNonNull(blockTime);
            requireNonNull(streamKeyWithId);
        }

        @Override
        public List<String> elements() {
            final var fixedValues = new ArrayList<String>();
            fixedValues.add(commandType);
            blockTime.ifPresent(fixedValues::add);
            final var mapValues = streamKeyWithId.entrySet()
                    .stream()
                    .flatMap(it -> Stream.of(it.getKey(), it.getValue()));
            return concat(fixedValues.stream(), mapValues).toList();
        }
    }
}
