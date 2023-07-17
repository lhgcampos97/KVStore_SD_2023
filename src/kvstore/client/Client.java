package kvstore.client;

import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the number of servers: ");
        int numServers = scanner.nextInt();
        scanner.nextLine(); // Consumes the newline character

        String[] serverAddresses = new String[numServers];

        for (int i = 0; i < numServers; i++) {
            System.out.print("Enter the IP address and port number of server " + (i + 1) + " (e.g., localhost:8000): ");
            serverAddresses[i] = scanner.nextLine();
        }

        System.out.print("Enter the key: ");
        String key = scanner.nextLine();

        System.out.print("Enter the command (PUT or GET): ");
        String command = scanner.nextLine();

        if (command.equalsIgnoreCase("PUT")) {
            System.out.print("Enter the value: ");
            String value = scanner.nextLine();

            String response = sendPutRequest(serverAddresses, key, value);
            System.out.println("Response: " + response);
        } else if (command.equalsIgnoreCase("GET")) {
            String response = sendGetRequest(serverAddresses, key);
            System.out.println("Response: " + response);
        } else {
            System.out.println("Invalid command");
        }
    }

    private static String sendPutRequest(String[] serverAddresses, String key, String value) {
        String response = "Error: No servers available";

        for (String serverAddress : serverAddresses) {
            String[] parts = serverAddress.split(":");
            String ipAddress = parts[0];
            int port = Integer.parseInt(parts[1]);

            try (
                Socket socket = new Socket(ipAddress, port);
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                String request = "PUT " + key + " " + value;
                writer.println(request);

                response = reader.readLine();
                if (!response.equals("Error: No servers available")) {
                    break;
                }
            } catch (IOException e) {
                // Server is not available
            }
        }

        return response;
    }

    private static String sendGetRequest(String[] serverAddresses, String key) {
        String response = "Error: No servers available";

        for (String serverAddress : serverAddresses) {
            String[] parts = serverAddress.split(":");
            String ipAddress = parts[0];
            int port = Integer.parseInt(parts[1]);

            try (
                Socket socket = new Socket(ipAddress, port);
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                String request = "GET " + key;
                writer.println(request);

                response = reader.readLine();
                if (!response.equals("Error: No servers available")) {
                    break;
                }
            } catch (IOException e) {
                // Server is not available
            }
        }

        return response;
    }
}
