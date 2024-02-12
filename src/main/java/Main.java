import java.io.IOException;
import java.net.ServerSocket;

public class Main {
    public static void main(String[] args) {
        final int port = 6379;
        try (final var serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            final var clientSocket = serverSocket.accept();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
