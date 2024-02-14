import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;

public class Main {
    public static void main(String[] args) {
        final int port = 6379;
        try (final var serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            // closing socket closes input and output streams
            try (final var clientSocket = serverSocket.accept()) {
                final var reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                final var writer = new PrintWriter(clientSocket.getOutputStream());
                String command;
                while ((command = reader.readLine()) != null) {
                    System.out.println("Command: " + command);
                    if (command.equalsIgnoreCase("PING")) {
                        responsePong(writer);
                    }
                }
            } catch (Exception exception) {
                System.out.println("Exception by the accept method has been thrown: " + exception.getMessage());
                throw new RuntimeException(exception);
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void responsePong(PrintWriter writer) {
        writer.print("+PONG\r\n");
        writer.flush();
    }
}
