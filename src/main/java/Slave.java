import resp.Decoder;
import resp.Encoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.net.InetAddress.getByName;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

final class Slave implements Server {
    private static final ExecutorService POOL = newFixedThreadPool(8);
    private final Configuration config;
    private final Decoder decoder;
    private final Encoder encoder;

    public Slave(
            Configuration configuration,
            Decoder decoder,
            Encoder encoder
    ) {
        this.config = requireNonNull(configuration);
        this.decoder = requireNonNull(decoder);
        this.encoder = requireNonNull(encoder);
    }

    public void connectToMaster() {
        POOL.execute(() -> {
            try (final var socket = new Socket(getByName(config.masterHost().get()), config.masterPort().get())) {

                final var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                final var writer = new PrintWriter(socket.getOutputStream());
                writer.print(encoder.encodeAsArray("PING"));
                writer.flush();
                receiveResponse(reader);

                while (!socket.isClosed()) {
                    writer.print(encoder.encodeAsArray(List.of("REPLCONF", "listening-port", String.valueOf(config.port()))));
                    writer.flush();

                    receiveResponse(reader);
                    writer.print(encoder.encodeAsArray(List.of("REPLCONF", "capa", "psync2")));
                    writer.flush();

                    receiveResponse(reader);
                    writer.print(encoder.encodeAsArray(List.of("PSYNC", "?", "-1")));
                    writer.flush();
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
            final var writer = new PrintWriter(socket.getOutputStream());
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

    // https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
    private void parseCommand(BufferedReader reader, PrintWriter writer) {
        try {
            while (!reader.ready()) {
            }
            final var array = reader.readLine();
            decoder.decodeBulkString(reader)
                    .ifPresent(command -> {
                        switch (command.data()) {
                            case "info" -> {
                                decoder.decodeBulkString(reader);
                                writeInfoResponse(writer);
                            }
                            default -> throw new UnsupportedOperationException(
                                    "Command on replica [%s] not implemented".formatted(command));
                        }
                    });
        } catch (IOException ignored) {
        }
    }

    private void receiveResponse(BufferedReader reader) throws IOException {
        decoder.decodeSimpleString(reader)
                .ifPresentOrElse(command -> {
                    switch (command.firstPart()) {
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

    private void writeInfoResponse(PrintWriter writer) {
        final var infoReplication = List.of(
                "# Replication",
                "role:slave",
                "master_repl_offset:0"
        );
        final var encodedInfo = encoder.encodeAsBulkString(infoReplication);
        writer.print(encodedInfo);
        writer.flush();
    }
}
