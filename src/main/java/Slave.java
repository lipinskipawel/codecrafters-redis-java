import db.Database;
import resp.Command;
import resp.Decoder;
import resp.Encoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.lang.Integer.parseInt;
import static java.net.InetAddress.getByName;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

final class Slave implements Server {
    private static final ExecutorService POOL = newFixedThreadPool(8);
    private final Configuration config;
    private final Database database;
    private final Decoder decoder;
    private final Encoder encoder;
    private int numberOfProcessedBytes;

    public Slave(
            Configuration configuration,
            Database database,
            Decoder decoder,
            Encoder encoder
    ) {
        this.config = requireNonNull(configuration);
        this.database = requireNonNull(database);
        this.decoder = requireNonNull(decoder);
        this.encoder = requireNonNull(encoder);
        this.numberOfProcessedBytes = 0;
    }

    public void connectToMaster() {
        POOL.execute(() -> {
            try (final var socket = new Socket(getByName(config.masterHost().get()), config.masterPort().get())) {

                final var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                final var writer = new PrintWriter(socket.getOutputStream());
                final var rawWriter = socket.getOutputStream();
                writer.print(encoder.encodeAsArray("PING"));
                writer.flush();
                receiveResponse(reader);

                writer.print(encoder.encodeAsArray(List.of("REPLCONF", "listening-port", String.valueOf(config.port()))));
                writer.flush();

                receiveResponse(reader);
                writer.print(encoder.encodeAsArray(List.of("REPLCONF", "capa", "psync2")));
                writer.flush();

                receiveResponse(reader);
                writer.print(encoder.encodeAsArray(List.of("PSYNC", "?", "-1")));
                writer.flush();
                receiveResponse(reader);

                receiveRdbFile(reader);

                while (!socket.isClosed()) {
                    decoder.parseCommand(reader)
                            .ifPresent(command -> {
                                switch (command) {
                                    case Command.Set set -> {
                                        set.expiryTime().ifPresentOrElse(
                                                it -> database.set(set.key(), set.value(), ofMillis(parseInt(it))),
                                                () -> database.set(set.key(), set.value())
                                        );
                                        updateReplicatedBytes(set);
                                    }
                                    case Command.Replconf replconf -> {
                                        if (replconf.elements().get(1).equalsIgnoreCase("getack")) {
                                            final var processedBytes = String.valueOf(numberOfProcessedBytes);
                                            writeReplconfAckResponse(rawWriter, List.of("REPLCONF", "ACK", processedBytes));
                                            updateReplicatedBytes(replconf);
                                        }
                                    }
                                    case Command.Ping ping -> updateReplicatedBytes(ping);
                                    default -> throw new IllegalStateException("Unexpected value: " + command);
                                }
                            });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void runServer() {
        try (final var serverSocket = new ServerSocket(config.port())) {
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

    private void handle(Socket socket) {
        try {
            final var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            final var writer = socket.getOutputStream();
            while (!socket.isClosed()) {
                parseCommand(reader, writer);
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

    private void parseCommand(BufferedReader reader, OutputStream writer) {
        decoder.parseCommand(reader)
                .ifPresent(command -> {
                    switch (command) {
                        case Command.Info ignored -> writeInfoResponse(writer);
                        case Command.Get get -> {
                            final var storedValue = database.get(get.value());
                            writeGetResponse(writer, storedValue);
                        }
                        default -> throw new UnsupportedOperationException(
                                "Command on replica [%s] not implemented".formatted(command));
                    }
                });
    }

    private void receiveResponse(BufferedReader reader) throws IOException {
        while (!reader.ready()) {
        }
        decoder.decodeSimpleString(reader)
                .ifPresentOrElse(command -> {
                    switch (command.split(" ")[0]) {
                        case "+PONG" -> System.out.println("Received response for PING");
                        case "+OK" -> System.out.println("Received response OK");
                        case "+FULLRESYNC" -> System.out.println("Received response for PSYNC");
                        default -> {
                            System.out.println("Received command is not implemented yet.");
                            throw new IllegalStateException("Unexpected value: " + command);
                        }
                    }
                }, () -> {
                    throw new IllegalArgumentException("Could not parse response from master");
                });
    }

    private void receiveRdbFile(BufferedReader reader) throws IOException {
        final var length = parseInt(reader.readLine().substring(1));
        final var rawRdbFile = new char[length - 1]; // I have no idea why * is included in the RDB file
        reader.read(rawRdbFile, 0, length - 1);
    }

    private void updateReplicatedBytes(Command command) {
        final var header = 3 + String.valueOf(command.elements().size()).length(); // *,\r\n

        final var payload = command.elements()
                .stream()
                .mapToInt(it -> {
                    final var firstRow = 3 + String.valueOf(it.length()).length(); // $,\r\n
                    return firstRow + it.length() + 2; // \r\n
                })
                .sum();

        numberOfProcessedBytes += (header + payload);
    }

    private void writeInfoResponse(OutputStream writer) {
        try {
            final var infoReplication = List.of(
                    "# Replication",
                    "role:slave",
                    "master_repl_offset:0"
            );
            final var encodedInfo = encoder.encodeAsBulkString(infoReplication);
            writer.write(encodedInfo.getBytes());
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeSetResponse(OutputStream writer) {
        writeAndFlush(writer, encoder.encodeAsSimpleString("OK"));
    }

    private void writeGetResponse(OutputStream writer, Optional<String> value) {
        writeAndFlush(writer, encoder.encodeAsBulkString(value));
    }

    private void writeReplconfAckResponse(OutputStream writer, List<String> values) {
        writeAndFlush(writer, encoder.encodeAsArray(values));
    }

    private void writeAndFlush(OutputStream writer, String toSend) {
        writeAndFlush(writer, toSend.getBytes());
    }

    private void writeAndFlush(OutputStream writer, byte[] toSend) {
        try {
            writer.write(toSend);
            writer.flush();
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }
}
