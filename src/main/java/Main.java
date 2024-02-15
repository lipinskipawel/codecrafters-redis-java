import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Runtime.getRuntime;

public class Main {
    private static final ExecutorService POOL = Executors.newFixedThreadPool(getRuntime().availableProcessors());

    public static void main(String[] args) {
        final int port = 6379;
        try (final var serverSocket = new ServerSocket(port)) {
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

    private static void handle(Socket socket) {
        try {
            final var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            final var writer = new PrintWriter(socket.getOutputStream());
            String command;
            while ((command = reader.readLine()) != null) {
                System.out.println("Command: " + command);
                if (command.equalsIgnoreCase("PING")) {
                    responsePong(writer);
                }
            }
        } catch (Exception exception) {
            System.out.println("Exception thrown, closing socket: " + exception.getMessage());
            try {
                // closing socket closes input and output streams
                socket.close();
            } catch (IOException e) {
                System.out.println("Exception by the accept method has been thrown: " + exception.getMessage());
                throw new RuntimeException(e);
            }
            throw new RuntimeException(exception);
        }
    }

    private static void responsePong(PrintWriter writer) {
        writer.print("+PONG\r\n");
        writer.flush();
    }
}
