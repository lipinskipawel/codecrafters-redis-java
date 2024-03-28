import db.Database;
import resp.Command;
import resp.Command.Config;
import resp.Command.Psync;
import resp.Decoder;
import resp.Encoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static resp.Command.Echo;
import static resp.Command.Get;
import static resp.Command.Info;
import static resp.Command.Ping;
import static resp.Command.Replconf;
import static resp.Command.Set;

final class Master implements Server {
    private static final ExecutorService POOL = newFixedThreadPool(16);
    private final Configuration config;
    private final Database database;
    private final Decoder decoder;
    private final Encoder encoder;
    private final Map<Socket, Long> replicasWithOffset;
    private final AtomicLong offset;

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
        this.replicasWithOffset = new ConcurrentHashMap<>();
        this.offset = new AtomicLong();
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
            while (!socket.isClosed()) {
                decoder.parseCommand(reader).ifPresent(it -> respondToCommand(socket, it));
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
        } finally {
            replicasWithOffset.remove(socket);
        }
    }

    private void respondToCommand(Socket socket, Command command) {
        switch (command) {
            // codecrafers.io assumes that PING does not have arguments
            case Ping ignored -> writePingResponse(socket);
            case Echo echo -> writeEchoResponse(socket, echo.argument());
            case Set set -> {
                set.expiryTime().ifPresentOrElse(
                        it -> database.set(set.key(), set.value(), ofMillis(parseInt(it))),
                        () -> database.set(set.key(), set.value())
                );
                writeSetResponse(socket);
                propagateCommand(set);
            }
            case Get get -> {
                final var storedValue = database.get(get.value());
                writeGetResponse(socket, storedValue);
            }
            case Info ignored -> writeInfoReplicaResponse(socket);
            case Replconf replconf -> {
                if (replconf.key().equalsIgnoreCase("ack")) {
                    replicasWithOffset.computeIfPresent(socket, (con, cur) -> cur + parseLong(replconf.value()));
                }
                if (replconf.key().equalsIgnoreCase("listening-port")
                        || replconf.key().equalsIgnoreCase("capa")) {
                    writeReplConfResponse(socket);
                }
            }
            case Psync ignored -> {
                writePsyncResponse(socket);
                replicasWithOffset.put(socket, 0L);
            }
            case Command.Wait wait -> {
                final var currentOffset = offset.get();
                if (currentOffset == 0) {
                    writeWaitResponse(socket, replicasWithOffset.size());
                    return;
                }
                updateOffset(new Replconf("REPLCONF", "GETACK", "*"));
                replicasWithOffset.keySet().forEach(this::sendGetAck);

                try {
                    runAsync(waitForReplicasToSync(parseLong(wait.numberOfReplica()), currentOffset))
                            .orTimeout(parseLong(wait.timeout()), MILLISECONDS)
                            .join();
                } catch (Exception e) {
                    System.out.println(e);
                }

                final var replicasInSync = replicasInSync(currentOffset);
                writeWaitResponse(socket, replicasInSync);
            }
            case Config configCommand -> writeConfigResponse(socket, configCommand, config);
            case Command.Type type -> writeTypeResponse(socket, database.type(type.key()));
            case Command.Xadd xadd -> {
                final var response = database.saveStream(xadd.streamKey(), xadd.streamKeyValue(), xadd.values())
                        .map(encoder::encodeAsBulkString, encoder::encodeAsError)
                        .actualValue();
                writeAndFlush(socket, response);
            }
            case Command.Xrange xrange -> {
                final var response = database.range(xrange.streamKey(), xrange.start(), xrange.end());
                final var encodedEntries = response.stream()
                        .map(it -> {
                            final var encodedId = encoder.encodeAsBulkString(it.id());
                            final var mapValues = it.pairs().entrySet()
                                    .stream()
                                    .flatMap(pair -> Stream.of(pair.getKey(), pair.getValue()))
                                    .toList();
                            final var encodedMap = encoder.encodeAsArray(mapValues);
                            return encoder.wrapContentAsArray(List.of(encodedId, encodedMap));
                        })
                        .toList();
                writeAndFlush(socket, encoder.wrapContentAsArray(encodedEntries));
            }
            case Command.Xread xread -> {
                final var response = database.xread(xread.streamKeyWithId());
                final var idWithEntries = response
                        .entrySet()
                        .stream()
                        .map(streamKeyWithId -> {
                            final var encodedStreamKey = encoder.encodeAsBulkString(streamKeyWithId.getKey());
                            final var encodedEntries = streamKeyWithId.getValue()
                                    .stream()
                                    .map(it -> {
                                        final var encodedId = encoder.encodeAsBulkString(it.id());
                                        final var mapValues = it.pairs().entrySet()
                                                .stream()
                                                .flatMap(pair -> Stream.of(pair.getKey(), pair.getValue()))
                                                .toList();
                                        final var encodedMap = encoder.encodeAsArray(mapValues);
                                        return encoder.wrapContentAsArray(List.of(encodedId, encodedMap));
                                    })
                                    .toList();
                            final var wrappedEntries = encoder.wrapContentAsArray(encodedEntries);
                            return List.of(encoder.wrapContentAsArray(List.of(encodedStreamKey, wrappedEntries)));
                        })
                        .flatMap(Collection::stream)
                        .toList();
                final var wrappedIdWithEntries = encoder.wrapContentAsArray(idWithEntries);
                writeAndFlush(socket, wrappedIdWithEntries);
            }
        }
    }

    private void sendGetAck(Socket socket) {
        runAsync(() -> writeAndFlush(socket, encoder.encodeAsArray(List.of("REPLCONF", "GETACK", "*"))));
    }

    private void updateOffset(Command command) {
        final var readBytes = updateReplicatedBytes(command);
        offset.addAndGet(readBytes);
    }

    private Runnable waitForReplicasToSync(long numOfReplicasThatMustBeInSync, long currentOffset) {
        return () -> {
            var replicasInSync = replicasInSync(currentOffset);
            while (numOfReplicasThatMustBeInSync >= replicasInSync) {
                replicasInSync = replicasInSync(currentOffset);
            }
        };
    }

    private long replicasInSync(long offset) {
        return replicasWithOffset.values()
                .stream()
                .filter(it -> it >= offset)
                .count();
    }

    private long updateReplicatedBytes(Command command) {
        final var header = 3 + String.valueOf(command.elements().size()).length(); // *,\r\n

        final var payload = command.elements()
                .stream()
                .mapToInt(it -> {
                    final var firstRow = 3 + String.valueOf(it.length()).length(); // $,\r\n
                    return firstRow + it.length() + 2; // \r\n
                })
                .sum();

        return header + payload;
    }

    private void propagateCommand(Command command) {
        updateOffset(command);
        replicasWithOffset.keySet().forEach(replica -> writeAndFlush(replica, encoder.encodeAsArray(command.elements())));
    }

    private void writePingResponse(Socket socket) {
        writeAndFlush(socket, encoder.encodeAsSimpleString("PONG"));
    }

    private void writeEchoResponse(Socket socket, String echoMessage) {
        writeAndFlush(socket, encoder.encodeAsBulkString(echoMessage));
    }

    private void writeSetResponse(Socket socket) {
        writeAndFlush(socket, encoder.encodeAsSimpleString("OK"));
    }

    private void writeGetResponse(Socket socket, Optional<String> value) {
        writeAndFlush(socket, encoder.encodeAsBulkString(value));
    }

    private void writeInfoReplicaResponse(Socket socket) {
        final var infoReplication = List.of(
                "# Replication",
                "role:master",
                "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                "master_repl_offset:" + offset);
        writeAndFlush(socket, encoder.encodeAsBulkString(infoReplication));
    }

    private void writeReplConfResponse(Socket socket) {
        writeAndFlush(socket, encoder.encodeAsSimpleString("OK"));
    }

    private void writePsyncResponse(Socket socket) {
        writeAndFlush(socket, encoder.encodeAsSimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"));
        final var decoded = Base64.getDecoder().decode(Database.EMPTY_DATABASE);
        writeAndFlush(socket, "$%s\r\n".formatted(decoded.length));
        writeAndFlush(socket, decoded);
    }

    private void writeWaitResponse(Socket socket, long numberOfReplicasInSync) {
        writeAndFlush(socket, encoder.encodeAsInteger(numberOfReplicasInSync));
    }

    private void writeConfigResponse(Socket socket, Config config, Configuration configuration) {
        if (config.value().equals("dir")) {
            writeAndFlush(socket, encoder.encodeAsArray(List.of(config.value(), configuration.directory().get())));
        }
        if (config.value().equals("dbfilename")) {
            writeAndFlush(socket, encoder.encodeAsArray(List.of(config.value(), configuration.file().get())));
        }
    }

    private void writeSaveStreamResponse(Socket socket, String value) {
        writeAndFlush(socket, encoder.encodeAsBulkString(value));
    }

    private void writeErrorStreamResponse(Socket socket, String value) {
        writeAndFlush(socket, encoder.encodeAsError(value));
    }

    private void writeTypeResponse(Socket socket, String type) {
        writeAndFlush(socket, encoder.encodeAsSimpleString(type));
    }

    private void writeAndFlush(Socket socket, String toSend) {
        writeAndFlush(socket, toSend.getBytes());
    }

    private void writeAndFlush(Socket socket, byte[] toSend) {
        try {
            final var writer = socket.getOutputStream();
            writer.write(toSend);
            writer.flush();
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }
}
