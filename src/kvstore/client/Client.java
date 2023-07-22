package kvstore.client;


import com.google.gson.Gson;
import kvstore.message.Message;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

public class Client {

	private static final Gson gson = new Gson();
    private static Map<String, Long> timestampStore = new HashMap<>(); // Map to store key-timestamp pairs


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

		boolean exit = false;

		do {
			System.out.print("Enter the key (or type 'exit' to quit): ");
			String key = scanner.nextLine();

			if (key.equalsIgnoreCase("exit")) {
				exit = true;
			} else {
				System.out.print("Enter the command (PUT or GET): ");
				String command = scanner.nextLine();

				if (command.equalsIgnoreCase("PUT")) {
					System.out.print("Enter the value: ");
					String value = scanner.nextLine();

					long timestamp = System.currentTimeMillis(); // Get the current timestamp
					
                    Message message = new Message(command, key, value, timestamp); // Pass the timestamp to the Message constructor
                    String response = sendRequest(serverAddresses, message);
                    System.out.println("Response: " + response);
                    
                    Message responseJson = gson.fromJson(response, Message.class);
                    timestampStore.put(responseJson.getKey(), responseJson.getTimestamp());
                    
				} else if (command.equalsIgnoreCase("GET")) {
					
					Long timestamp = timestampStore.getOrDefault(key, 0L);
					
					Message message = new Message(command, key, null, timestamp); // Pass 0 as the timestamp for GET requests
                    String response = sendRequest(serverAddresses, message);
                    System.out.println("Response: " + response);

                    Message responseJson = gson.fromJson(response, Message.class);
                    timestampStore.put(responseJson.getKey(), responseJson.getTimestamp());
                    
				} else {
					System.out.println("Invalid command");
				}
			}
		} while (!exit);
	}

	private static String sendRequest(String[] serverAddresses, Message message) {
		Random random = new Random();
		String randomServerAddress = serverAddresses[random.nextInt(serverAddresses.length)];

		String response = "Error: No servers available";

		String[] parts = randomServerAddress.split(":");
		String ipAddress = parts[0];
		int port = Integer.parseInt(parts[1]);


		try (
				Socket socket = new Socket(ipAddress, port);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
				) {
			String jsonMessage = gson.toJson(message);
			writer.println(jsonMessage);

			response = reader.readLine();
			if (!response.equals("Error: No servers available")) {
				return response;
			}
		} catch (IOException e) {
			// Server is not available
		}


		return response;
	}


}
