package kvstore.server;

import com.google.gson.Gson;
import kvstore.message.Message;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Server {
    private ServerSocket serverSocket;
    private Map<String, String> store;
    private Map<String, Long> timestamps;
    private Map<String, String> serverAddresses;
    private boolean isLeader;
    private static final Gson gson = new Gson();

    public Server(boolean isLeader) {
        this.isLeader = isLeader;
        store = new HashMap<>();
        timestamps = new HashMap<>();
        serverAddresses = new HashMap<>();
    }

    public void start(String ipAddress, int port, String leaderIp, int leaderPort) {
        try {
            serverSocket = new ServerSocket(port, 10, InetAddress.getByName(ipAddress));
            System.out.println("Server started on " + ipAddress + ":" + port);

            if (isLeader) {
                // Add leader's address to the table
                serverAddresses.put(ipAddress + ":" + port, "Leader");

                // Read secondary server IP and port from the keyboard
                Scanner scanner = new Scanner(System.in);
                System.out.print("Enter the number of secondary servers: ");
                int numSecondaryServers = scanner.nextInt();
                scanner.nextLine(); // Consumes the newline character

                for (int i = 0; i < numSecondaryServers; i++) {
                    System.out.print("Enter the IP address and port number of secondary server " + (i + 1) + " (e.g., localhost:8001): ");
                    String secondaryServerAddress = scanner.nextLine();
                    serverAddresses.put(secondaryServerAddress, "Secondary");
                }
            }

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New connection from " + clientSocket.getInetAddress());

                // Create a new thread to handle the client connection
                ClientHandler clientHandler = new ClientHandler(clientSocket, leaderIp, leaderPort);
                clientHandler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Read IP and port from the keyboard
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the IP address: ");
        String ipAddress = scanner.nextLine();
        System.out.print("Enter the port number: ");
        int port = scanner.nextInt();
        scanner.nextLine(); // Consumes the newline character

        System.out.print("Enter the IP address of the leader: ");
        String leaderIp = scanner.nextLine();
        System.out.print("Enter the port number of the leader: ");
        int leaderPort = scanner.nextInt();

        boolean isLeader = false;

        if (ipAddress.equals(leaderIp) && port == leaderPort) {
            isLeader = true;
            System.out.print("This server is the leader! \n");
        }

        Server server = new Server(isLeader);
        server.start(ipAddress, port, leaderIp, leaderPort);
    }

    private class ClientHandler extends Thread {
        private Socket clientSocket;
        private String leaderIp;
        private int leaderPort;

        public ClientHandler(Socket clientSocket, String leaderIp, int leaderPort) {
            this.clientSocket = clientSocket;
            this.leaderIp = leaderIp;
            this.leaderPort = leaderPort;
        }

        public void run() {
            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
            ) {
            	String jsonRequest = reader.readLine();
                Message request = gson.fromJson(jsonRequest, Message.class);
                System.out.println("Received request: " + jsonRequest);
                
                String command = request.getCommand();
                String key = request.getKey();
                String value = request.getValue();
                Long timestamp = request.getTimestamp();

                Message response;

                if ("GET".equals(command)) {
                    response = handleGet(key,timestamp);
                } else if ("PUT".equals(command)) {
                    if (isLeader) {
                        sendReplication(request);
                        response = handlePut(key, value, timestamp); // Pass the received timestamp to the handlePut method

                    } else {
                        // Encaminhe a requisição para o líder
                        response = forwardRequestToLeader(jsonRequest);
                    }
                } else if ("REPLICATION".equals(command)) {
                    response = handleReplication(key, value, timestamp); // Pass the received timestamp to the handleReplication method
                } else {
                    response =  new Message("Invalid Command",key, value, timestamp);
                }
                
                writer.println(gson.toJson(response));
                System.out.println("Sent response: " + gson.toJson(response));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        

        private void sendReplication(Message request) {
            for (String serverAddress : serverAddresses.keySet()) {
                if (serverAddress.equals(leaderIp + ":" + leaderPort)) {
                    continue; // Skip the leader itself
                }

                String[] parts = serverAddress.split(":");
                String ipAddress = parts[0];
                int port = Integer.parseInt(parts[1]);

                try (
                        Socket socket = new Socket(ipAddress, port);
                        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)
                ) {
                    // Create a replication message
                    Message replicationMessage = new Message("REPLICATION", request.getKey(), request.getValue(),request.getTimestamp());
                    String jsonMessage = gson.toJson(replicationMessage);
                    writer.println(jsonMessage);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private Message forwardRequestToLeader(String jsonRequest) {
            try (
                    Socket leaderSocket = new Socket(leaderIp, leaderPort);
                    PrintWriter writer = new PrintWriter(leaderSocket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(leaderSocket.getInputStream()))
            ) {
                writer.println(jsonRequest);
                String jsonResponse = reader.readLine();
                return gson.fromJson(jsonResponse,Message.class);
            } catch (IOException e) {
                e.printStackTrace();
                Message request = gson.fromJson(jsonRequest, Message.class);
                String key = request.getKey();
                String value = request.getValue();
                Long timestamp = request.getTimestamp();
                
                Message errorMessage = new Message("Error forwarding request to leader", key, value, timestamp);
                return errorMessage;
            }
        }

        private Message handleGet(String key, long clientTimestamp) {
            String value = store.get(key);
            long serverTimestamp = timestamps.getOrDefault(key, 0L); // Get the stored timestamp or use 0 if not found

            if (value != null) {
                if (clientTimestamp < serverTimestamp) {
                    // O timestamp do cliente é menor que o timestamp armazenado no servidor
                    Message tryOtherServerOrLaterMessage = new Message("TRY_OTHER_SERVER_OR_LATER", key, "", serverTimestamp);
                    return tryOtherServerOrLaterMessage;
                } else {
                    // O timestamp do cliente é maior ou igual ao timestamp armazenado no servidor
                    Message getMessage = new Message("GET_OK", key, value, serverTimestamp);
                    return getMessage;
                }
            } else {
                Message getMessage = new Message("NULL", key, "", 0L);
                return getMessage;
            }
        }

        private Message handlePut(String key, String value, long timestamp) {
            store.put(key, value);
            timestamps.put(key, timestamp); // Update the timestamp for the key
            Message putMessage = new Message("PUT_OK", key, value, timestamp);
            return putMessage;
        }

        private Message handleReplication(String key, String value, long timestamp) {
            // Handle the replication message received from the leader
            store.put(key, value);
            timestamps.put(key, timestamp); // Update the timestamp for the key
            Message replicationMessage = new Message("REPLICATION_OK", key, value, timestamp);
            return replicationMessage;
        }
    }
}
