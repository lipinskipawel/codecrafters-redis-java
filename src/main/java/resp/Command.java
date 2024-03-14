package resp;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public sealed interface Command {

    record Ping(String commandType) implements Command {
        public Ping {
            requireNonNull(commandType);
        }
    }

    record Echo(String commandType, String echoArgument) implements Command {
        public Echo {
            requireNonNull(commandType);
            requireNonNull(echoArgument);
        }
    }

    record Set(String commandType, String key, String value, Optional<String> expiryTime) implements Command {
        public Set {
            requireNonNull(commandType);
            requireNonNull(key);
            requireNonNull(value);
            requireNonNull(expiryTime);
        }
    }

    record Get(String commandType, String value) implements Command {
        public Get {
            requireNonNull(commandType);
            requireNonNull(value);
        }
    }

    record Info(String commandType) implements Command {
        public Info {
            requireNonNull(commandType);
        }
    }

    record Replconf(String commandType, String first, String second) implements Command {
        public Replconf {
            requireNonNull(commandType);
            requireNonNull(first);
            requireNonNull(second);
        }
    }

    record Psync(String commandType, String replicationId, String offset) implements Command {
        public Psync {
            requireNonNull(commandType);
            requireNonNull(replicationId);
            requireNonNull(offset);
        }
    }
}
