import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        final int port = 6379;
        try (final var serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            final var clientSocket = serverSocket.accept();
            responsePong(clientSocket);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void responsePong(Socket socket) {
        try (var writer = new PrintWriter(socket.getOutputStream(), true)) {
            writer.print("+PONG\r\n");
        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
