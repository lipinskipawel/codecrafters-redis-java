import resp.Decoder;
import resp.Encoder;

public class Main {
    private static final Encoder ENCODER = new Encoder();
    private static final Decoder DECODER = new Decoder();
    private static final Database DATABASE = new Database();

    public static void main(String[] args) {
        final var config = Configuration.parseCommandLineArguments(args);

        final var server = runServer(config);
        server.runServer();
    }

    private static Server runServer(Configuration config) {
        if (config.role().equals("slave")) {
            final var slave = new Slave(config, DATABASE, DECODER, ENCODER);
            slave.connectToMaster();
            return slave;
        }
        return new Master(config, DATABASE, DECODER, ENCODER);
    }
}
