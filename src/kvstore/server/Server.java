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
    private boolean isLeader;
    private static final Gson gson = new Gson();

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
            	String jsonRequest = reader.readLine();
                Message request = gson.fromJson(jsonRequest, Message.class);
                System.out.println("Received request: " + jsonRequest);

                String response;
                if (isLeader) {
                    // O servidor atual é o líder, manipule a requisição localmente
                    if ("GET".equals(request.getCommand())) {
                        response = handleGet(request.getKey());
                    } else if ("PUT".equals(request.getCommand())) {
                        response = handlePut(request.getKey(), request.getValue());
                    } else {
                        response = "Invalid command";
                    }
                } else {
                    // Encaminhe a requisição para o líder
                    String jsonResponse = forwardRequestToLeader(jsonRequest);
                    response = gson.fromJson(jsonResponse, String.class);
                }

                writer.println(gson.toJson(response));
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

        private String forwardRequestToLeader(String jsonRequest) {
            try (
                Socket leaderSocket = new Socket(leaderIp, leaderPort);
                PrintWriter writer = new PrintWriter(leaderSocket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(leaderSocket.getInputStream()))
            ) {
                writer.println(jsonRequest);
                String jsonResponse = reader.readLine();
                return jsonResponse;
            } catch (IOException e) {
                e.printStackTrace();
                return gson.toJson("Error forwarding request to leader");
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
