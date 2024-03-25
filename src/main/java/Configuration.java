import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

final class Configuration {
    private final int port;
    private final String role;
    private Optional<String> masterHost;
    private Optional<Integer> masterPort;
    private Optional<String> directory;
    private Optional<String> file;

    private Configuration(
            int port,
            String role,
            Optional<String> masterHost,
            Optional<Integer> masterPort,
            Optional<String> directory,
            Optional<String> file
    ) {
        this.port = port;
        this.role = requireNonNull(role);
        this.masterHost = requireNonNull(masterHost);
        this.masterPort = requireNonNull(masterPort);
        this.directory = requireNonNull(directory);
        this.file = requireNonNull(file);
    }

    public static Configuration parseCommandLineArguments(String[] args) {
        final var port = portToStartServer(args);
        final var role = roleOfServer(args);
        final var masterHost = masterHost(args);
        final var masterPort = masterPort(args);
        final var directory = directory(args);
        final var file = file(args);
        return new Configuration(port, role, masterHost, masterPort, directory, file);
    }

    public int port() {
        return port;
    }

    public String role() {
        return role;
    }

    public Optional<String> masterHost() {
        return masterHost;
    }

    public Optional<Integer> masterPort() {
        return masterPort;
    }

    public Optional<String> directory() {
        return directory;
    }

    public Optional<String> file() {
        return file;
    }

    private static int portToStartServer(String[] args) {
        return findIndex(args, "--port")
                .map(it -> it + 1)
                .map(it -> args[it])
                .map(Integer::parseInt)
                .orElse(6379);
    }

    private static String roleOfServer(String[] args) {
        return asList(args).contains("--replicaof") ? "slave" : "master";
    }

    private static Optional<String> masterHost(String[] args) {
        return findIndex(args, "--replicaof")
                .map(it -> it + 1)
                .map(it -> args[it]);
    }

    private static Optional<Integer> masterPort(String[] args) {
        return findIndex(args, "--replicaof")
                .map(it -> it + 2)
                .map(it -> args[it])
                .map(Integer::parseInt);
    }

    private static Optional<String> directory(String[] args) {
        return findIndex(args, "--dir")
                .map(it -> it + 1)
                .map(it -> args[it]);
    }

    private static Optional<String> file(String[] args) {
        return findIndex(args, "--dbfilename")
                .map(it -> it + 1)
                .map(it -> args[it]);
    }

    private static Optional<Integer> findIndex(String[] args, String name) {
        for (var i = 0; i < args.length; i++) {
            if (args[i].equals(name)) {
                return of(i);
            }
        }
        return empty();
    }
}
