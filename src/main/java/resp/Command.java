package resp;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

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
}
