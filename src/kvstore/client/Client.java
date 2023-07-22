package kvstore.client;


import com.google.gson.Gson;
import kvstore.message.Message;

import java.io.*;
import java.net.*;
import java.util.Random;
import java.util.Scanner;

public class Client {

	private static final Gson gson = new Gson();

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

					Message message = new Message(command, key, value);
					String response = sendRequest(serverAddresses, message);
					System.out.println("Response: " + response);
				} else if (command.equalsIgnoreCase("GET")) {
					Message message = new Message(command, key, null);
					String response = sendRequest(serverAddresses, message);
					System.out.println("Response: " + response);
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
