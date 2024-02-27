import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.lang.Integer.parseInt;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {
    private static final ExecutorService POOL = newFixedThreadPool(8);
    private static final Database DATABASE = new Database();

    public static void main(String[] args) {
        final int port = 6379;
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
                DATABASE.set(key, value);
                yield of(new Set());
            }
            case "get" -> {
                final var keyToLookUp = parseBulkString(reader).orElseThrow();
                final var storedValue = DATABASE.get(keyToLookUp);
                yield of(new Get(storedValue));
            }
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
        String dataType = null;
        try {
            dataType = reader.readLine();
            final var numberOfBytes = parseInt(dataType.substring(1));
            final var command = reader.readLine();
            return of(command);
        } catch (IOException ioException) {
            System.out.printf("ioException during reading from socket [%s]%n", ioException.getMessage());
            return empty();
        } catch (Exception exception) {
            System.out.printf("exception during parsing bulk string [%s]%n", exception.getMessage());
            return ofNullable(dataType);
        }
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
        private final String value;

        Get(String value) {
            this.value = value;
        }

        @Override
        public void execute(PrintWriter writer) {
            if (value != null) {
                writer.print("$" + value.length() + "\r\n");
                writer.print(value + "\r\n");
                writer.flush();
                return;
            }
            writer.print("$-1\r\n");
            writer.flush();
        }
    }
}
