import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.lang.Integer.parseInt;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {
    private static final ExecutorService POOL = newFixedThreadPool(8);
    private static final Database DATABASE = new Database();

    public static void main(String[] args) {
        final var port = portToStartServer(args);
        try (final var serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                final var clientSocket = serverSocket.accept();
                POOL.execute(() -> handle(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            POOL.close();
            throw new RuntimeException(e);
        }
    }

    private static void handle(Socket socket) {
        try {
            final var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            final var writer = new PrintWriter(socket.getOutputStream());
            while (!socket.isClosed()) {
                parseCommand(reader).ifPresent(it -> it.execute(writer));
            }
            System.out.println("Socket was closed");
        } catch (Exception exception) {
            System.out.println("Exception thrown, closing socket: " + exception.getMessage());
            try {
                // closing socket closes input and output streams
                socket.close();
            } catch (IOException e) {
                System.out.println("Exception by the accept method has been thrown: " + exception.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    // https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
    private static Optional<Command> parseCommand(BufferedReader reader) {
        final var command = commandType(reader);
        return switch (command) {
            case "ping" -> of(new Ping()); // codecrafers.io assumes that PING does not have arguments
            case "echo" -> {
                final var argumentToEcho = parseBulkString(reader)
                        .orElseThrow(() -> new IllegalArgumentException("Echo command must have argument"));
                yield of(new Echo(argumentToEcho));
            }
            case "set" -> {
                final var key = parseBulkString(reader).orElseThrow();
                final var value = parseBulkString(reader).orElseThrow();
                final var expireMark = parseBulkString(reader);
                // this is rly bad and should be refactored
                // initial idea can be around creating proper object with all information in it instead of using String
                // as an artificial command in the switch expression
                if (expireMark.isPresent()) {
                    final var expireTime = parseBulkString(reader).orElseThrow();
                    DATABASE.set(key, value, ofMillis(parseInt(expireTime)));
                } else {
                    DATABASE.set(key, value);
                }
                yield of(new Set());
            }
            case "get" -> {
                final var keyToLookUp = parseBulkString(reader).orElseThrow();
                final var storedValue = DATABASE.get(keyToLookUp);
                yield of(new Get(storedValue));
            }
            case "info" -> of(new Info("master"));
            case null -> empty();
            default -> throw new UnsupportedOperationException("command [%s] not implemented".formatted(command));
        };
    }

    private static String commandType(BufferedReader reader) {
        try {
            final var line = reader.readLine();
            if (line == null) {
                return null;
            }
            return parseBulkString(reader)
                    .orElseThrow(() -> new IllegalArgumentException("First element must be present"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Optional<String> parseBulkString(BufferedReader reader) {
        try {
            if (!reader.ready()) {
                return empty();
            }
            final var dataType = reader.readLine();
            final var numberOfBytes = parseInt(dataType.substring(1));
            final var command = reader.readLine();
            return of(command);
        } catch (IOException ioException) {
            System.out.printf("ioException during reading from socket [%s]%n", ioException.getMessage());
            return empty();
        } catch (Exception exception) {
            System.out.printf("exception during parsing bulk string [%s]%n", exception.getMessage());
            return empty();
        }
    }

    private static int parseInteger(BufferedReader reader) {
        try {
            final var colon = reader.readLine();
            if (!colon.equals(":")) {
                throw new IllegalArgumentException("Expected [:] as a first byte when parsing integer, instead [%s]".formatted(colon));
            }
            return parseInt(reader.readLine());
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    private static int portToStartServer(String[] args) {
        if (args.length == 0) {
            return 6379;
        }
        return parseInt(args[1]);
    }

    /**
     * Not sure if this abstraction is a good idea.
     * Probably this should be rewritten into some sort of 'encoders' package.
     * Inside encoders, I could have wrapper methods like asSimpleString, asBulkString...
     */
    private interface Command {
        void execute(PrintWriter writer);
    }

    private static class Ping implements Command {

        @Override
        public void execute(PrintWriter writer) {
            writer.print("+PONG\r\n");
            writer.flush();
        }
    }

    private static class Echo implements Command {
        private final String echoMessage;

        Echo(String echoMessage) {
            this.echoMessage = echoMessage;
        }

        @Override
        public void execute(PrintWriter writer) {
            writer.print("$" + echoMessage.length() + "\r\n");
            writer.print(echoMessage + "\r\n");
            writer.flush();
        }
    }

    private static class Set implements Command {

        @Override
        public void execute(PrintWriter writer) {
            writer.print("+OK\r\n");
            writer.flush();
        }
    }

    private static class Get implements Command {
        private final Optional<String> value;

        Get(Optional<String> value) {
            this.value = requireNonNull(value);
        }

        @Override
        public void execute(PrintWriter writer) {
            final var bulkStringResponse = value
                    .map(it -> "$" + it.length() + "\r\n" + it + "\r\n")
                    .orElse("$-1\r\n");
            writer.print(bulkStringResponse);
            writer.flush();
        }
    }

    private static class Info implements Command {
        private final String info;

        Info(String role) {
            this.info = "role:" + role;
        }

        @Override
        public void execute(PrintWriter writer) {
            writer.print("$" + info.length() + "\r\n");
            writer.print(info + "\r\n");
            writer.flush();
        }
    }
}
