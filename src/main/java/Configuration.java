import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

final class Configuration {
    private final int port;
    private final String role;

    private Configuration(int port, String role) {
        this.port = port;
        this.role = requireNonNull(role);
    }

    public static Configuration parseCommandLineArguments(String[] args) {
        final var port = portToStartServer(args);
        final var role = roleOfServer(args);
        return new Configuration(port, role);
    }

    public int port() {
        return port;
    }

    public String role() {
        return role;
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
}
