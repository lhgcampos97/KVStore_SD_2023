package kvstore.server;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Server {
    private ServerSocket serverSocket;
    private Map<String, String> store;
    private boolean isLeader;

    public Server(boolean isLeader) {
    	this.isLeader = isLeader;
        store = new HashMap<>();
    }

    public void start(String ipAddress, int port, String leaderIp, int leaderPort) {
        try {
            serverSocket = new ServerSocket(port, 10, InetAddress.getByName(ipAddress));
            System.out.println("Server started on " + ipAddress + ":" + port);

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
                String request = reader.readLine();
                System.out.println("Received request: " + request);

                String response;
                if (isLeader == true) {
                    // O servidor atual é o líder, manipule a requisição localmente
                    if (request.startsWith("GET")) {
                        String key = request.split(" ")[1];
                        response = handleGet(key);
                    } else if (request.startsWith("PUT")) {
                        String[] parts = request.split(" ", 3);
                        String key = parts[1];
                        String value = parts[2];
                        response = handlePut(key, value);
                    } else {
                        response = "Invalid command";
                    }
                } else {
                    // Encaminhe a requisição para o líder
                    response = forwardRequestToLeader(request);
                }

                writer.println(response);
                System.out.println("Sent response: " + response);
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

        private String forwardRequestToLeader(String request) {
            try (
                Socket leaderSocket = new Socket(leaderIp, leaderPort);
                PrintWriter writer = new PrintWriter(leaderSocket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(leaderSocket.getInputStream()))
            ) {
                writer.println(request);
                String response = reader.readLine();
                return response;
            } catch (IOException e) {
                e.printStackTrace();
                return "Error forwarding request to leader";
            }
        }

        private String handleGet(String key) {
            String value = store.get(key);
            if (value != null) {
                return "OK " + value;
            } else {
                return "Key not found";
            }
        }

        private String handlePut(String key, String value) {
            store.put(key, value);
            return "OK";
        }
    }
}
