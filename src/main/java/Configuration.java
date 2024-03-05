import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

final class Configuration {
    private final int port;
    private final String role;
    private Optional<String> masterHost;
    private Optional<Integer> masterPort;

    private Configuration(int port, String role, Optional<String> masterHost, Optional<Integer> masterPort) {
        this.port = port;
        this.role = requireNonNull(role);
        this.masterHost = requireNonNull(masterHost);
        this.masterPort = requireNonNull(masterPort);
    }

    public static Configuration parseCommandLineArguments(String[] args) {
        final var port = portToStartServer(args);
        final var role = roleOfServer(args);
        final var masterHost = masterHost(args);
        final var masterPort = masterPort(args);
        return new Configuration(port, role, masterHost, masterPort);
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

    private static int portToStartServer(String[] args) {
        if (args.length == 0) {
            return 6379;
        }
        return parseInt(args[1]);
    }

    private static String roleOfServer(String[] args) {
        return asList(args).contains("--replicaof") ? "slave" : "master";
    }

    private static Optional<String> masterHost(String[] args) {
        try {
            return of(args[3]);
        } catch (IndexOutOfBoundsException e) {
            return empty();
        }
    }

    private static Optional<Integer> masterPort(String[] args) {
        try {
            return of(parseInt(args[4]));
        } catch (IndexOutOfBoundsException e) {
            return empty();
        }
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "port=" + port +
                ", role='" + role + '\'' +
                ", masterHost=" + masterHost +
                ", masterPort=" + masterPort +
                '}';
    }
}
