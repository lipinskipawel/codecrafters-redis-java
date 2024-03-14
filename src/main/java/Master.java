import resp.Command;
import resp.Command.Psync;
import resp.Decoder;
import resp.Encoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.lang.Integer.parseInt;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static resp.Command.Echo;
import static resp.Command.Get;
import static resp.Command.Info;
import static resp.Command.Ping;
import static resp.Command.Replconf;
import static resp.Command.Set;

final class Master implements Server {
    private static final ExecutorService POOL = newFixedThreadPool(8);
    private final Configuration config;
    private final Database database;
    private final Decoder decoder;
    private final Encoder encoder;

    public Master(
            Configuration configuration,
            Database database,
            Decoder decoder,
            Encoder encoder
    ) {
        this.config = requireNonNull(configuration);
        this.database = requireNonNull(database);
        this.decoder = requireNonNull(decoder);
        this.encoder = requireNonNull(encoder);
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
                decoder.parseCommand(reader).ifPresent(it -> respondToCommand(writer, it));
            }
            System.out.println("Socket was closed");
        } catch (Exception exception) {
            System.out.println("Exception thrown, closing socket: " + exception);
            try {
                // closing socket closes input and output streams
                socket.close();
            } catch (IOException e) {
                System.out.println("Exception by the accept method has been thrown: " + exception.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    private void respondToCommand(OutputStream writer, Command command) {
        switch (command) {
            // codecrafers.io assumes that PING does not have arguments
            case Ping ignored -> writePingResponse(writer);
            case Echo echo -> writeEchoResponse(writer, echo.echoArgument());
            case Set set -> {
                set.expiryTime().ifPresentOrElse(
                        it -> database.set(set.key(), set.value(), ofMillis(parseInt(it))),
                        () -> database.set(set.key(), set.value())
                );
                writeSetResponse(writer);
            }
            case Get get -> {
                final var storedValue = database.get(get.value());
                writeGetResponse(writer, storedValue);
            }
            case Info ignored -> writeInfoReplicaResponse(writer);
            case Replconf ignored -> writeReplConfResponse(writer);
            case Psync ignored -> writePsyncResponse(writer);
        }
    }

    private void writePingResponse(OutputStream writer) {
        writeAndFlush(writer, encoder.encodeAsSimpleString("PONG"));
    }

    private void writeEchoResponse(OutputStream writer, String echoMessage) {
        writeAndFlush(writer, encoder.encodeAsBulkString(echoMessage));
    }

    private void writeSetResponse(OutputStream writer) {
        writeAndFlush(writer, encoder.encodeAsSimpleString("OK"));
    }

    private void writeGetResponse(OutputStream writer, Optional<String> value) {
        writeAndFlush(writer, encoder.encodeAsBulkString(value));
    }

    private void writeInfoReplicaResponse(OutputStream writer) {
        final var infoReplication = List.of(
                "# Replication",
                "role:master",
                "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                "master_repl_offset:0");
        writeAndFlush(writer, encoder.encodeAsBulkString(infoReplication));
    }

    private void writeReplConfResponse(OutputStream writer) {
        writeAndFlush(writer, encoder.encodeAsSimpleString("OK"));
    }

    private void writePsyncResponse(OutputStream writer) {
        writeAndFlush(writer, encoder.encodeAsSimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"));
        final var decoded = Base64.getDecoder().decode(Database.EMPTY_DATABASE);
        writeAndFlush(writer, "$%s\r\n".formatted(decoded.length));
        writeAndFlush(writer, decoded);
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
