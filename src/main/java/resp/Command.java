package resp;

import static java.util.Objects.requireNonNull;

public sealed interface Command {

    record SimpleString(String simpleString) implements Command {
        public SimpleString {
            requireNonNull(simpleString);
        }

        public String firstPart() {
            return parts()[0];
        }

        private String[] parts() {
            return simpleString.split(" ");
        }
    }

    record BulkString(String bulkString) implements Command {
        public BulkString {
            requireNonNull(bulkString);
        }

        public String data() {
            return bulkString;
        }
    }
}
