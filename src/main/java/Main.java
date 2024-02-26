import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;
import static java.util.Optional.empty;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {
    private static final ExecutorService POOL = newFixedThreadPool(getRuntime().availableProcessors());
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
            String readLine;
            while ((readLine = reader.readLine()) != null) {
                final var commands = parseCommand(readLine, reader);
                for (var command : commands) {
                    command.execute(writer);
                }
            }
        } catch (Exception exception) {
            System.out.println("Exception thrown, closing socket: " + exception.getMessage());
            try {
                // closing socket closes input and output streams
                socket.close();
            } catch (IOException e) {
                System.out.println("Exception by the accept method has been thrown: " + exception.getMessage());
                throw new RuntimeException(e);
            }
            throw new RuntimeException(exception);
        }
    }

    // https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
    private static List<Command> parseCommand(String line, BufferedReader reader) {
        try {
            final var commands = new ArrayList<Command>();
            final var numberOfElements = parseInt(line.substring(1));
            for (var i = 0; i < numberOfElements; i++) {
                final var command = parseBulkString(reader)
                        .orElseThrow(() -> new IllegalArgumentException("First element must be present"));
                if (command.equalsIgnoreCase("PING")) {
                    // for codecrafers.io purpose PING does not accept any other arguments
                    commands.add(new Ping());
                }
                if (command.equalsIgnoreCase("ECHO")) {
                    final var argumentToEcho = parseBulkString(reader)
                            .orElseThrow(() -> new IllegalArgumentException("Echo command must have argument"));
                    // better impl would avoid this and read second argument by continuing looping
                    // or by removing the loop and reading commands as objects (recursion would be helpful)
                    i++;
                    commands.add(new Echo(argumentToEcho));
                }
                if (command.equalsIgnoreCase("SET")) {
                    final var key = parseBulkString(reader).orElseThrow();
                    final var value = parseBulkString(reader).orElseThrow();
                    i++;
                    i++;
                    DATABASE.set(key, value);
                    commands.add(new Set());
                }
                if (command.equalsIgnoreCase("GET")) {
                    final var keyToLookUp = parseBulkString(reader).orElseThrow();
                    final var storedValue = DATABASE.get(keyToLookUp);
                    i++;
                    commands.add(new Get(storedValue));
                }
            }
            return commands;
        } catch (Exception e) {
            System.out.println("Exception during parsing client command");
            return List.of();
        }
    }

    private static Optional<String> parseBulkString(BufferedReader reader) {
        try {
            final var dataType = reader.readLine();
            if (dataType.charAt(0) != '$') {
                throw new IllegalArgumentException("Expected [$] got [%s]".formatted(dataType.charAt(0)));
            }
            final var numberOfBytes = parseInt(dataType.substring(1));
            final var command = reader.readLine();
            return Optional.of(command);
        } catch (IOException ioException) {
            return empty();
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
